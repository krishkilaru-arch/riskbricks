# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 GDELT Daily Ingestion (Bronze)
# MAGIC
# MAGIC filter to the `company_universe`, and write to Bronze with safe `replaceWhere`.
# MAGIC
# MAGIC **Outputs**:
# MAGIC - `riskbricks.bronze.historical_news_gdelt` (partitioned by `event_date`, `symbol`)

# COMMAND ----------

# Widgets
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import zipfile
import io
import csv
import re

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
except Exception:
    print("ℹ️ autoMerge config not supported in this environment; continuing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Load Company Universe

# COMMAND ----------

symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM {catalog}.gold.company_universe
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_to_company = {row.symbol: row.company_name for row in symbols_df.collect()}
symbol_to_sector = {row.symbol: row.sector for row in symbols_df.collect()}

print(f"📊 GDELT ingestion for {len(portfolio_symbols)} symbols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Keyword Map (for GDELT filtering)

# COMMAND ----------

COMPANY_KEYWORDS = {
    "AAPL": ["APPLE"],
    "MSFT": ["MICROSOFT"],
    "GOOGL": ["GOOGLE", "ALPHABET"],
    "AMZN": ["AMAZON"],
    "TSLA": ["TESLA"],
    "NVDA": ["NVIDIA"],
    "META": ["FACEBOOK"],
    "NFLX": ["NETFLIX"],
    "COST": ["COSTCO"],
}

keyword_map = {}
for symbol in portfolio_symbols:
    keywords = set()
    sym_upper = (symbol or "").upper()
    if sym_upper:
        keywords.add(sym_upper)

    if symbol in COMPANY_KEYWORDS:
        keywords.update([kw.upper() for kw in COMPANY_KEYWORDS[symbol] if kw])
    else:
        company_name = symbol_to_company.get(symbol, symbol) or ""
        tokens = re.split(r"\s+", company_name.upper())
        for token in tokens:
            token = token.replace(".", "").replace(",", "").replace("'S", "")
            token = token.replace("INC", "").replace("CORP", "").strip()
            if len(token) >= 4:
                keywords.add(token)

    keyword_map[symbol] = sorted([kw for kw in keywords if kw])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Date Range (defaults to yesterday ET)

# COMMAND ----------

start_date_input = dbutils.widgets.get("start_date").strip()
end_date_input = dbutils.widgets.get("end_date").strip()

def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

local_tz = ZoneInfo("America/New_York")

if not start_date_input or not end_date_input:
    yesterday = datetime.now(local_tz).date() - timedelta(days=1)
    start_date = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=local_tz)
    end_date = start_date
    print(
        f"ℹ️ start_date/end_date not provided; defaulting to yesterday ET: {start_date.strftime('%Y-%m-%d')}"
    )
else:
    start_date = _parse_date(start_date_input).replace(tzinfo=local_tz)
    end_date = _parse_date(end_date_input).replace(tzinfo=local_tz)

if end_date < start_date:
    raise ValueError("end_date must be on or after start_date.")


print(f"📅 Date range (ET): {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌐 URL Generators

# COMMAND ----------

def _date_range(start_dt, end_dt):
    current = start_dt
    while current <= end_dt:
        yield current
        current += timedelta(days=1)

def get_event_url(date_obj):
    date_str = date_obj.strftime("%Y%m%d")
    return date_str, f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"

    date_str = date_obj.strftime("%Y%m%d")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Process GDELT Events (Daily)

# COMMAND ----------

def _match_symbols(text_blob, keyword_map):
    matches = []
    text_upper = (text_blob or "").upper()
    for symbol, keywords in keyword_map.items():
        for kw in keywords:
            if len(kw) >= 4 and kw in text_upper:
                matches.append(symbol)
                break
    return matches

def process_event_day(date_str, url, keyword_map):
    rows = []
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"❌ {date_str}: HTTP {resp.status_code}")
            return rows

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"), delimiter="\t")
                for row in reader:
                    if len(row) < 35:
                        continue
                    try:
                        event_id = row[0]
                        event_date = row[1]
                        actor1_name = row[6]
                        actor2_name = row[16]
                        goldstein_scale = float(row[30]) if row[30] else 0.0
                        num_mentions = int(row[31]) if row[31] else 0
                        num_sources = int(row[32]) if row[32] else 0
                        num_articles = int(row[33]) if row[33] else 0
                        avg_tone = float(row[34]) if row[34] else 0.0
                        source_url = row[60] if len(row) > 60 else None

                        text_blob = f"{actor1_name} {actor2_name}"
                        matched_symbols = _match_symbols(text_blob, keyword_map)
                        if not matched_symbols:
                            continue

                        for symbol in matched_symbols:
                            rows.append({
                                "event_id": str(event_id),
                                "event_date": event_date,
                                "symbol": symbol,
                                "company_name": symbol_to_company.get(symbol, symbol),
                                "sector": symbol_to_sector.get(symbol, "Unknown"),
                                "actor1_name": actor1_name,
                                "actor2_name": actor2_name,
                                "goldstein_scale": goldstein_scale,
                                "num_mentions": num_mentions,
                                "num_sources": num_sources,
                                "num_articles": num_articles,
                                "avg_tone": avg_tone,
                                "source_url": source_url,
                                "source_file_date": date_str,
                                "ingestion_timestamp": datetime.now(local_tz),
                            })
                    except Exception:
                        continue
    except Exception as exc:
        print(f"❌ {date_str}: {exc}")
        return rows

    print(f"✅ {date_str}: {len(rows)} events")
    return rows

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

    def safe_get(idx):
        return parts[idx] if len(parts) > idx else ""

    return {
        "event_date": safe_get(1),
        "source_collection_id": safe_get(2),
        "source_common_name": safe_get(3),
        "document_identifier": safe_get(4),
        "themes": safe_get(6),
        "persons": safe_get(8),
        "organizations": safe_get(9),
        "tone": safe_get(10),
        "raw_record": "\t".join(parts[:20]),
    }

    rows = []
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"❌ {date_str}: HTTP {resp.status_code}")
            return rows

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                for line in f:
                    try:
                        parts = line.decode("utf-8", errors="ignore").strip().split("\t")
                        text_blob = " ".join([
                            fields.get("themes", ""),
                            fields.get("organizations", ""),
                            fields.get("persons", ""),
                            fields.get("document_identifier", ""),
                            fields.get("source_common_name", ""),
                        ])
                        matched_symbols = _match_symbols(text_blob, keyword_map)
                        if not matched_symbols:
                            continue

                        for symbol in matched_symbols:
                            rows.append({
                                "event_date": fields["event_date"],
                                "symbol": symbol,
                                "company_name": symbol_to_company.get(symbol, symbol),
                                "sector": symbol_to_sector.get(symbol, "Unknown"),
                                "source_common_name": fields["source_common_name"],
                                "document_identifier": fields["document_identifier"],
                                "themes": fields["themes"],
                                "persons": fields["persons"],
                                "organizations": fields["organizations"],
                                "tone": fields["tone"],
                                "raw_record": fields["raw_record"],
                                "source_file_date": date_str,
                                "ingestion_timestamp": datetime.now(local_tz),
                            })
                    except Exception:
                        continue
    except Exception as exc:
        print(f"❌ {date_str}: {exc}")
        return rows

    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write Helpers (replaceWhere)

# COMMAND ----------

def write_partitioned_table(table_name, df, start_dt, end_dt, partition_cols=("event_date", "symbol")):
    partition_col = partition_cols[0]
    replace_where = f"{partition_col} >= '{start_dt.strftime('%Y-%m-%d')}' AND {partition_col} <= '{end_dt.strftime('%Y-%m-%d')}'"
    df = df.filter(
        (F.col(partition_col) >= F.lit(start_dt.strftime('%Y-%m-%d')).cast("date")) &
        (F.col(partition_col) <= F.lit(end_dt.strftime('%Y-%m-%d')).cast("date"))
    )

    if not spark.catalog.tableExists(table_name):
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Run Ingestion

# COMMAND ----------

all_events = []

for day in _date_range(start_date, end_date):
    date_str, url = get_event_url(day)
    all_events.extend(process_event_day(date_str, url, keyword_map))


if not all_events:
    dbutils.notebook.exit("No GDELT events found for this range.")

events_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("actor1_name", StringType(), True),
    StructField("actor2_name", StringType(), True),
    StructField("goldstein_scale", DoubleType(), True),
    StructField("num_mentions", IntegerType(), True),
    StructField("num_sources", IntegerType(), True),
    StructField("num_articles", IntegerType(), True),
    StructField("avg_tone", DoubleType(), True),
    StructField("source_url", StringType(), True),
    StructField("source_file_date", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
])

events_df = spark.createDataFrame(all_events, schema=events_schema)
events_df = events_df \
    .withColumn("event_date_parsed", F.expr("try_to_date(CAST(event_date AS STRING), 'yyyyMMdd')")) \
    .withColumn("source_file_date_parsed", F.expr("try_to_date(CAST(source_file_date AS STRING), 'yyyyMMdd')")) \
    .withColumn(
        "event_date",
        F.coalesce(F.col("event_date_parsed"), F.last_day(F.col("source_file_date_parsed"))).cast("date")
    ) \
    .withColumn("date_is_reliable", F.col("event_date_parsed").isNotNull()) \
    .drop("event_date_parsed", "source_file_date_parsed")


    StructField("event_date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("source_common_name", StringType(), True),
    StructField("document_identifier", StringType(), True),
    StructField("themes", StringType(), True),
    StructField("persons", StringType(), True),
    StructField("organizations", StringType(), True),
    StructField("tone", StringType(), True),
    StructField("raw_record", StringType(), True),
    StructField("source_file_date", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
])

    .withColumn("event_date_parsed", F.expr("try_to_date(substr(CAST(event_date AS STRING), 1, 8), 'yyyyMMdd')")) \
    .withColumn("source_file_date_parsed", F.expr("try_to_date(CAST(source_file_date AS STRING), 'yyyyMMdd')")) \
    .withColumn(
        "event_date",
        F.coalesce(F.col("event_date_parsed"), F.last_day(F.col("source_file_date_parsed"))).cast("date")
    ) \
    .withColumn("date_is_reliable", F.col("event_date_parsed").isNotNull()) \
    .drop("event_date_parsed", "source_file_date_parsed")

    .filter(F.col("document_identifier").rlike("^https?://")) \
    .groupBy("event_date", "symbol") \

events_df = events_df \

events_table = f"{catalog}.bronze.historical_news_gdelt"
write_partitioned_table(events_table, events_df, start_date, end_date)
spark.sql(f"COMMENT ON TABLE {events_table} IS 'GDELT historical news events filtered by company_universe'")

print(f"✅ Saved events to {events_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Quick Profile (Bronze GDELT)

# COMMAND ----------

events_profile = spark.sql(f"""
    SELECT
      symbol,
      COUNT(*) AS rows,
      MIN(event_date) AS min_date,
      MAX(event_date) AS max_date
    FROM {events_table}
    GROUP BY symbol
    ORDER BY symbol
""")
events_profile.show(50, truncate=False)


    SELECT
      symbol,
      COUNT(*) AS rows,
      MIN(event_date) AS min_date,
      MAX(event_date) AS max_date
    GROUP BY symbol
    ORDER BY symbol
""")

dbutils.notebook.exit(f"✅ GDELT ingestion complete: {len(all_events)} events")
