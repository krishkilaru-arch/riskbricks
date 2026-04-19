# Databricks notebook source
# Widgets (run this cell first)
dbutils.widgets.text(
    "date_list",
    "2025-05-01,2025-05-02,2025-05-03,2025-05-04,2025-05-05,2025-05-06,2025-05-07,2025-05-08,2025-05-09,2025-05-10,2025-05-11,2025-05-12,2025-05-13,2025-05-14,2025-05-15,2025-05-16,2025-05-17,2025-05-18,2025-05-19,2025-05-20,2025-05-21,2025-05-22,2025-05-23,2025-05-24,2025-05-25,2025-05-26,2025-05-27,2025-05-28,2025-05-29,2025-05-30,2025-06-14,2025-06-15,2025-06-16,2025-06-17,2025-06-18,2025-06-19,2025-06-20,2025-06-21,2025-06-22,2025-06-23,2025-06-24,2025-06-25,2025-06-26,2025-06-27,2025-06-28,2025-06-29,2025-06-30,2025-07-01,2025-08-01,2025-08-02,2025-08-03,2025-08-04,2025-08-05,2025-08-06,2025-08-07,2025-08-08,2025-08-09,2025-08-10,2025-08-11,2025-08-12,2025-08-13,2025-08-14,2025-08-15,2025-08-16,2025-08-17,2025-08-18,2025-08-19,2025-08-20,2025-08-21,2025-08-22,2025-08-23,2025-08-24,2025-08-25,2025-08-26,2025-08-27,2025-08-28,2025-08-29",
    "Date list (YYYY-MM-DD, comma-separated)"
)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
import requests
import zipfile
import io
import re

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# Get all symbols from company universe
symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_map = {row.symbol: row.company_name for row in symbols_df.collect()}

# Hardcoded keywords for major companies (tested to work with GDELT)
COMPANY_KEYWORDS = {
    "AAPL": ["APPLE"],
    "MSFT": ["MICROSOFT"],
    "GOOGL": ["GOOGLE", "ALPHABET"],
    "GOOG": ["GOOGLE", "ALPHABET"],
    "AMZN": ["AMAZON"],
    "TSLA": ["TESLA"],
    "NVDA": ["NVIDIA"],
    "META": ["FACEBOOK"],  # "META" is too common
    "NFLX": ["NETFLIX"],
    "JPM": ["JPMORGAN", "CHASE"],
    "BAC": ["BANK OF AMERICA"],
    "GS": ["GOLDMAN SACHS", "GOLDMAN"],
    "WMT": ["WALMART"],
    "XOM": ["EXXON"],
    "CVX": ["CHEVRON"],
    "BA": ["BOEING"],
    "DIS": ["DISNEY"],
    "SBUX": ["STARBUCKS"],
    "PFE": ["PFIZER"],
    "MA": ["MASTERCARD"],
    "JNJ": ["JOHNSON"],
    "UNH": ["UNITEDHEALTH"],
    "HD": ["HOME DEPOT"],
    "PG": ["PROCTER"],
    "KO": ["COCA-COLA", "COCA COLA"],
    "PEP": ["PEPSI", "PEPSICO"],
    "MCD": ["MCDONALD"],
    "NKE": ["NIKE"],
    "COST": ["COSTCO"],
    "ABT": ["ABBOTT"],
    "TMO": ["THERMO FISHER"],
    "LLY": ["ELI LILLY", "LILLY"],
    "ABBV": ["ABBVIE"],
    "MRK": ["MERCK"],
    "BMY": ["BRISTOL-MYERS", "BRISTOL MYERS"],
    "AMGN": ["AMGEN"],
    "GILD": ["GILEAD"],
    "CAT": ["CATERPILLAR"],
    "DE": ["DEERE"],
    "MMM": ["3M"],  # Might be too short
    "HON": ["HONEYWELL"],
    "UPS": ["UPS"],  # Might be too short
    "RTX": ["RAYTHEON"],
    "LMT": ["LOCKHEED"],
    "GD": ["GENERAL DYNAMICS"],
    "NOC": ["NORTHROP"],
    "CRM": ["SALESFORCE"],
    "ADBE": ["ADOBE"],
    "ORCL": ["ORACLE"],
    "CSCO": ["CISCO"],  # Note: might match "SAN FRANCISCO"
    "IBM": ["IBM"],  # Might be too short
    "INTC": ["INTEL"],  # Note: might match "INTELLIGENCE"
    "AMD": ["AMD"],  # Might be too short
    "QCOM": ["QUALCOMM"],
    "TXN": ["TEXAS INSTRUMENTS"],
    "AVGO": ["BROADCOM"],
    "PYPL": ["PAYPAL"],
    "SQ": ["SQUARE", "BLOCK"],  # Block Inc now
    "UBER": ["UBER"],
    "ABNB": ["AIRBNB"],
    "COIN": ["COINBASE"],
    "F": ["FORD MOTOR", "FORD"],
    "GM": ["GENERAL MOTORS"],
    "T": ["AT&T"],  # Might be too short
    "VZ": ["VERIZON"],
    "TMUS": ["T-MOBILE", "TMOBILE"],
    "CMCSA": ["COMCAST"],
}

keyword_map = {}
for symbol in portfolio_symbols:
    keywords = set()
    sym_upper = symbol.upper()
    if sym_upper:
        keywords.add(sym_upper)

    if symbol in COMPANY_KEYWORDS:
        keywords.update([kw.upper() for kw in COMPANY_KEYWORDS[symbol] if kw])
    else:
        company_name = symbol_map.get(symbol, symbol) or ""
        tokens = re.split(r"\s+", company_name.upper())
        for token in tokens:
            token = token.replace(".", "").replace(",", "").replace("'S", "")
            token = token.replace("INC", "").replace("CORP", "")
            token = token.strip()
            if len(token) >= 4:
                keywords.add(token)

    keyword_map[symbol] = sorted([kw for kw in keywords if kw])

print(f"📊 Fetching ad-hoc GKG for {len(portfolio_symbols)} stocks")

# COMMAND ----------

def extract_gkg_fields(row):
    def safe_get(idx):
        return row[idx] if len(row) > idx else ""

    return {
        "gkg_record_id": safe_get(0),
        "event_date": safe_get(1),
        "source_collection_id": safe_get(2),
        "source_common_name": safe_get(3),
        "document_identifier": safe_get(4),
        "counts": safe_get(5),
        "themes": safe_get(6),
        "locations": safe_get(7),
        "persons": safe_get(8),
        "organizations": safe_get(9),
        "tone": safe_get(10),
        "enhanced_dates": safe_get(11),
        "gcam": safe_get(12),
        "sharing_image": safe_get(13),
        "raw_record": "\t".join(row[:20])
    }


def process_gkg_file(date_str, url, keywords_map):
    relevant = []
    try:
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            print(f"❌ {date_str} HTTP {response.status_code}")
            return relevant

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                for line in f:
                    try:
                        row = line.decode("utf-8", errors="ignore").strip().split("\t")
                        fields = extract_gkg_fields(row)

                        text_blob = " ".join([
                            fields.get("themes", ""),
                            fields.get("organizations", ""),
                            fields.get("persons", ""),
                            fields.get("document_identifier", ""),
                            fields.get("source_common_name", "")
                        ]).upper()

                        matched_symbols = []
                        for sym, kws in keywords_map.items():
                            for kw in kws:
                                if kw and kw.upper() in text_blob:
                                    matched_symbols.append(sym)
                                    break

                        if matched_symbols:
                            relevant.append({
                                "gkg_record_id": fields["gkg_record_id"],
                                "event_date": fields["event_date"],
                                "source_file_date": date_str,
                                "source_common_name": fields["source_common_name"],
                                "document_identifier": fields["document_identifier"],
                                "themes": fields["themes"],
                                "persons": fields["persons"],
                                "organizations": fields["organizations"],
                                "tone": fields["tone"],
                                "matched_symbols": matched_symbols,
                                "raw_record": fields["raw_record"],
                                "ingestion_timestamp": datetime.now()
                            })
                    except Exception:
                        continue
    except Exception as exc:
        print(f"❌ {date_str} error: {exc}")
        return relevant

    print(f"✅ {date_str} matched {len(relevant)} records")
    return relevant

# COMMAND ----------

date_list_raw = dbutils.widgets.get("date_list").strip()
if not date_list_raw:
    raise ValueError("Please provide date_list in YYYY-MM-DD, comma-separated.")

date_list = []
for item in date_list_raw.split(","):
    item = item.strip()
    if not item:
        continue
    date_list.append(datetime.strptime(item, "%Y-%m-%d").date())

date_list = sorted(set(date_list))
date_str_list = [d.strftime("%Y%m%d") for d in date_list]

print(f"📅 Ad-hoc GKG dates: {len(date_list)}")

# COMMAND ----------

missing_urls = []
gkg_table = f"{catalog}.bronze.historical_news_gdelt_gkg"

for date_str in date_str_list:
    url = f"http://data.gdeltproject.org/gkg/{date_str}.gkg.csv.zip"
    daily_gkg = process_gkg_file(date_str, url, keyword_map)
    if len(daily_gkg) == 0:
        missing_urls.append(date_str)
        continue

    # Build daily DataFrame (avoid accumulating all days in memory)
    gkg_df = spark.createDataFrame(pd.DataFrame(daily_gkg))
    gkg_exploded = gkg_df.withColumn("symbol", F.explode(F.col("matched_symbols"))) \
        .drop("matched_symbols")

    gkg_with_company = gkg_exploded.join(
        symbols_df,
        on="symbol",
        how="left"
    )

    gkg_final = gkg_with_company \
        .withColumn("event_date_parsed", F.expr("try_to_date(substr(CAST(event_date AS STRING), 1, 8), 'yyyyMMdd')")) \
        .withColumn("gkg_record_date_parsed", F.expr("try_to_date(substr(CAST(gkg_record_id AS STRING), 1, 8), 'yyyyMMdd')")) \
        .withColumn("source_file_date_parsed", F.expr("try_to_date(substr(CAST(source_file_date AS STRING), 1, 8), 'yyyyMMdd')")) \
        .withColumn(
            "event_date",
            F.coalesce(
                F.col("event_date_parsed"),
                F.col("gkg_record_date_parsed"),
                F.col("source_file_date_parsed"),
                F.last_day(F.col("source_file_date_parsed"))
            ).cast("date")
        ) \
        .withColumn(
            "date_is_reliable",
            F.coalesce(F.col("event_date_parsed"), F.col("gkg_record_date_parsed")).isNotNull()
        ) \
        .drop("event_date_parsed", "gkg_record_date_parsed", "source_file_date_parsed") \
        .withColumn("gkg_record_id", F.col("gkg_record_id").cast("string")) \
        .withColumn("source_common_name", F.col("source_common_name").cast("string")) \
        .withColumn("document_identifier", F.col("document_identifier").cast("string")) \
        .withColumn("themes", F.col("themes").cast("string")) \
        .withColumn("persons", F.col("persons").cast("string")) \
        .withColumn("organizations", F.col("organizations").cast("string")) \
        .withColumn("tone", F.col("tone").cast("string")) \
        .withColumn("raw_record", F.col("raw_record").cast("string"))

    gkg_to_save = gkg_final.select(
        F.col("gkg_record_id"),
        F.col("event_date"),
        F.col("symbol"),
        F.col("company_name"),
        F.col("sector"),
        F.col("source_common_name"),
        F.col("document_identifier"),
        F.col("themes"),
        F.col("persons"),
        F.col("organizations"),
        F.col("tone"),
        F.col("raw_record"),
        F.col("source_file_date").cast("string"),
        F.col("date_is_reliable").cast("boolean"),
        F.col("ingestion_timestamp")
    )

    # Filter to the target day and overwrite that partition only
    day_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    gkg_to_save = gkg_to_save.filter(F.col("event_date") == F.lit(day_date).cast("date"))

    if not spark.catalog.tableExists(gkg_table):
        gkg_to_save.write \
            .mode("overwrite") \
            .partitionBy("event_date", "symbol") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gkg_table)
    else:
        replace_where = f"event_date = '{day_date}'"
        gkg_to_save.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .saveAsTable(gkg_table)

print("✅ Ad-hoc GKG load complete.")
if missing_urls:
    print(f"⚠️  Missing/empty GKG files: {', '.join(missing_urls)}")

gkg_total = spark.sql(f"SELECT COUNT(*) as count FROM {gkg_table}").collect()[0]['count']
print(f"✅ Total rows in {gkg_table}: {gkg_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

