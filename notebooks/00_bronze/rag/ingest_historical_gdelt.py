# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 Historical News from GDELT (Configurable Range)
# MAGIC
# MAGIC **Purpose**: Fetch historical news for portfolio stocks for a configurable date range
# MAGIC
# MAGIC **Data Source**: GDELT Project (Global Database of Events, Language, and Tone)
# MAGIC - **Range**: Configurable via start_date/end_date (e.g., full year 2025)
# MAGIC - **Coverage**: 250M+ global news events
# MAGIC - **Cost**: 100% FREE
# MAGIC
# MAGIC **Output**: `riskbricks.bronze.historical_news`
# MAGIC
# MAGIC **Run Time**: 2-4 hours (one-time load)

# COMMAND ----------

# Widgets (run this cell first)
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install google-cloud-bigquery db-dtypes pandas-gbq requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
import json
from urllib.parse import quote
import zipfile
import io
import re

# Database setup
catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

print(f"✅ Using catalog: {catalog}")

# Enable schema auto-merge for new columns
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Get Portfolio Stock Symbols

# COMMAND ----------

# Get all symbols from company universe
symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_map = {row.symbol: row.company_name for row in symbols_df.collect()}

# Build simple keyword map - just use first significant word of company name
# GDELT typically uses names like "APPLE INC", "MICROSOFT", "GOOGLE", etc.
import re

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

# -------------------------------------------------------------------
# Optional: Include GDELT GKG (Global Knowledge Graph) for richer text-like data
# -------------------------------------------------------------------
include_gkg = True

# Build keyword_map for all portfolio symbols
# Always include the symbol, plus company name tokens when available.
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

    # Always keep at least the symbol to satisfy full universe coverage
    keyword_map[symbol] = sorted([kw for kw in keywords if kw])

print(f"📊 Fetching historical news for {len(portfolio_symbols)} stocks")
print(f"📊 Symbols: {', '.join(portfolio_symbols[:20])}..." if len(portfolio_symbols) > 20 else ', '.join(portfolio_symbols))
print(f"📊 Sample keywords: AAPL -> {keyword_map.get('AAPL', [])}, GOOGL -> {keyword_map.get('GOOGL', [])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌐 GDELT Access Method 1: Direct CSV Download (No Google Account Needed)
# MAGIC
# MAGIC GDELT publishes daily event files. We'll download and filter them.

# COMMAND ----------

def get_gdelt_event_urls(start_date, end_date):
    """
    Generate list of GDELT event file URLs for date range
    GDELT file naming: YYYYMMDD.export.CSV.zip
    """
    urls = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        url = f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"
        urls.append((date_str, url))
        current_date += timedelta(days=1)
    
    return urls

def get_gdelt_gkg_urls(start_date, end_date):
    """
    Generate list of GDELT GKG file URLs for date range.
    GKG files are daily, named like YYYYMMDD.gkg.csv.zip
    """
    urls = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        url = f"http://data.gdeltproject.org/gkg/{date_str}.gkg.csv.zip"
        urls.append((date_str, url))
        current_date += timedelta(days=1)
    return urls

def extract_gkg_fields(row):
    """
    Extract key fields from a GKG row (tab-delimited).
    Returns a dict with safe defaults.
    """
    # GKG 2.1 has many columns; we guard by index length.
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
    """
    Download and process a GKG file; return relevant rows matching company keywords.
    """
    relevant = []
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
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
    except Exception:
        return relevant

    return relevant

# -------------------------------------------------------------------
# Historical load configuration (Date Picker Widgets)
# -------------------------------------------------------------------
# Use widgets to pick a custom start/end date, or just set a year.

start_date_input = dbutils.widgets.get("start_date").strip()
end_date_input = dbutils.widgets.get("end_date").strip()

def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

if not start_date_input or not end_date_input:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = datetime(yesterday.year, yesterday.month, yesterday.day)
    end_date = start_date
    print(
        f"ℹ️ start_date/end_date not provided; defaulting to yesterday UTC: {start_date.strftime('%Y-%m-%d')}"
    )
else:
    start_date = _parse_date(start_date_input)
    end_date = _parse_date(end_date_input)

if end_date < start_date:
    raise ValueError("end_date must be on or after start_date.")

sample_urls = get_gdelt_event_urls(start_date, end_date)
print(f"📅 Generated {len(sample_urls)} daily file URLs")
print(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"📅 Example: {sample_urls[0][1]}")

if include_gkg:
    gkg_urls = get_gdelt_gkg_urls(start_date, end_date)
    print(f"📅 Generated {len(gkg_urls)} daily GKG file URLs")
    print(f"📅 GKG Example: {gkg_urls[0][1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Download and Process GDELT Events

# COMMAND ----------

import io
import zipfile
import csv

def download_and_parse_gdelt_day(date_str, url, target_symbols):
    """
    Download one day of GDELT events and filter for our stocks
    
    Returns: List of relevant events mentioning our stocks
    """
    try:
        print(f"  📥 Downloading {date_str}...", end='')
        
        # Download the ZIP file
        response = requests.get(url, timeout=60)
        
        if response.status_code != 200:
            print(f" ❌ Failed (HTTP {response.status_code})")
            return []
        
        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                csv_content = f.read().decode('utf-8', errors='ignore')
        
        # Parse CSV and filter for our stocks
        relevant_events = []
        reader = csv.reader(io.StringIO(csv_content), delimiter='\t')
        
        for row in reader:
            if len(row) < 35:  # Need at least through AvgTone (column 34)
                continue
            
            try:
                # Extract key fields (GDELT column positions)
                event_id = row[0]  # GLOBALEVENTID
                event_date = row[1]  # SQLDATE (YYYYMMDD)
                actor1_name = row[6]  # Actor1Name
                actor2_name = row[16]  # Actor2Name
                goldstein_scale = float(row[30]) if row[30] else 0.0  # GoldsteinScale
                num_mentions = int(row[31]) if row[31] else 0  # NumMentions
                num_sources = int(row[32]) if row[32] else 0  # NumSources
                num_articles = int(row[33]) if row[33] else 0  # NumArticles
                avg_tone = float(row[34]) if row[34] else 0.0  # AvgTone (-10 to +10)
                source_url = row[60] if len(row) > 60 else None  # SOURCEURL
                
                # Check if any of our company keywords are mentioned
                text_to_check = f"{actor1_name} {actor2_name}".upper()
                
                matched_symbols = []
                for symbol in target_symbols:
                    # Get keywords for this symbol
                    keywords = keyword_map.get(symbol, [symbol])
                    
                    # Check if any keyword matches
                    for keyword in keywords:
                        if len(keyword) >= 4 and keyword in text_to_check:  # Min 4 chars to avoid false positives
                            matched_symbols.append(symbol)
                            break  # Found a match, move to next symbol
                
                if matched_symbols:
                    relevant_events.append({
                        'event_id': event_id,
                        'event_date': event_date,
                        'source_file_date': date_str,
                        'actor1_name': actor1_name,
                        'actor2_name': actor2_name,
                        'goldstein_scale': goldstein_scale,
                        'num_mentions': num_mentions,
                        'num_sources': num_sources,
                        'num_articles': num_articles,
                        'avg_tone': avg_tone,
                        'source_url': source_url,
                        'matched_symbols': matched_symbols,
                        'ingestion_timestamp': datetime.now()
                    })
            
            except (ValueError, IndexError) as e:
                continue  # Skip malformed rows
        
        print(f" ✅ Found {len(relevant_events)} relevant events")
        return relevant_events
    
    except Exception as e:
        print(f" ❌ Error: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Process Historical Data for Configured Date Range

# COMMAND ----------

print(f"📰 Processing GDELT data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
print(f"⏱️  Total days: {len(sample_urls)}")
print("⏱️  This will take several hours for a full year")
print("⏱️  Progress updates every 100 events")
print("")

all_events = []
days_processed = 0

for date_str, url in sample_urls:  # Process ALL days
    events = download_and_parse_gdelt_day(date_str, url, portfolio_symbols)
    all_events.extend(events)
    days_processed += 1
    
    # Progress update every 100 events or every 365 days
    if len(all_events) > 0 and len(all_events) % 100 == 0:
        print(f"   💾 Accumulated {len(all_events)} events so far... (Day {days_processed}/{len(sample_urls)})")
    elif days_processed % 365 == 0:
        print(f"   📅 Processed {days_processed}/{len(sample_urls)} days, {len(all_events)} events so far...")

print(f"\n✅ Total events collected: {len(all_events)}")
print(f"✅ Days processed: {days_processed}")

# -------------------------------------------------------------------
# Optional: Process GDELT GKG (text-like metadata)
# -------------------------------------------------------------------
gkg_events = []
if include_gkg:
    print("")
    print(f"📰 Processing GDELT GKG data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    print(f"⏱️  Total days: {len(gkg_urls)}")
    print("⏱️  This will take several hours for a full year")
    print("")

    gkg_days_processed = 0
    for date_str, url in gkg_urls:
        daily_gkg = process_gkg_file(date_str, url, keyword_map)
        gkg_events.extend(daily_gkg)
        gkg_days_processed += 1

        if gkg_days_processed % 365 == 0:
            print(f"   📅 Processed {gkg_days_processed}/{len(gkg_urls)} days, {len(gkg_events)} GKG records so far...")

    print(f"\n✅ Total GKG records collected: {len(gkg_events)}")
    print(f"✅ GKG days processed: {gkg_days_processed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Bronze Layer

# COMMAND ----------

%md
### ⚠️ One-Time Rebuild (Run Manually If Needed)
Use this only once to rebuild tables with partitions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One-time rebuild (manual run only)
# MAGIC -- Table drops removed; use overwrite/replaceWhere when needed.

# COMMAND ----------

if len(all_events) == 0:
    print("⚠️  No events found. This might mean:")
    print("   1. GDELT data not available for these dates")
    print("   2. Stock symbols not mentioned in news")
    print("   3. Network issues")
    print("\n   Try adjusting date range or stock symbols.")
    dbutils.notebook.exit("No events to save")

# Convert to DataFrame
events_df = spark.createDataFrame(pd.DataFrame(all_events))

# Explode matched_symbols array so each event-symbol pair is a row
events_exploded = events_df.withColumn("symbol", F.explode(F.col("matched_symbols"))) \
    .drop("matched_symbols")

# Add company info
events_with_company = events_exploded.join(
    symbols_df,
    on="symbol",
    how="left"
)

# Convert event_date to proper date and cast all columns to correct types
# Use try_to_date; fall back to source_file_date when event_date is invalid
events_final = events_with_company \
    .withColumn("event_date_parsed", F.expr("try_to_date(CAST(event_date AS STRING), 'yyyyMMdd')")) \
    .withColumn("source_file_date_parsed", F.expr("try_to_date(CAST(source_file_date AS STRING), 'yyyyMMdd')")) \
    .withColumn(
        "event_date",
        F.coalesce(F.col("event_date_parsed"), F.last_day(F.col("source_file_date_parsed"))).cast("date")
    ) \
    .withColumn("date_is_reliable", F.col("event_date_parsed").isNotNull()) \
    .drop("event_date_parsed", "source_file_date_parsed") \
    .withColumn("event_id", F.col("event_id").cast("string")) \
    .withColumn("actor1_name", F.col("actor1_name").cast("string")) \
    .withColumn("actor2_name", F.col("actor2_name").cast("string")) \
    .withColumn("goldstein_scale", F.col("goldstein_scale").cast("double")) \
    .withColumn("num_mentions", F.col("num_mentions").cast("int")) \
    .withColumn("num_sources", F.col("num_sources").cast("int")) \
    .withColumn("num_articles", F.col("num_articles").cast("int")) \
    .withColumn("avg_tone", F.col("avg_tone").cast("double")) \
    .withColumn("source_url", F.col("source_url").cast("string")) \
    .withColumn("source_file_date", F.col("source_file_date").cast("string"))

# Show sample
print("📰 Sample events:")
events_final.select("event_date", "symbol", "company_name", "avg_tone", "num_articles", "actor1_name").show(10, truncate=60)

# COMMAND ----------

# Save to table
table_name = f"{catalog}.bronze.historical_news_gdelt"

# Select only the columns we need in the correct order
events_to_save = events_final.select(
    F.col("event_id").cast("string"),
    F.col("event_date"),
    F.col("symbol").cast("string"),
    F.col("company_name").cast("string"),
    F.col("sector").cast("string"),
    F.col("actor1_name").cast("string"),
    F.col("actor2_name").cast("string"),
    F.col("goldstein_scale").cast("double"),
    F.col("num_mentions").cast("int"),
    F.col("num_sources").cast("int"),
    F.col("num_articles").cast("int"),
    F.col("avg_tone").cast("double"),
    F.col("source_url").cast("string"),
    F.col("source_file_date").cast("string"),
    F.col("date_is_reliable").cast("boolean"),
    F.col("ingestion_timestamp")
)

def write_partitioned_table(table_name, df, start_dt, end_dt, partition_cols=("event_date", "symbol")):
    """Write only the selected date range; no table drops here."""
    partition_col = partition_cols[0]
    # Ensure only rows inside the replaceWhere range are written
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
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
            if not detail.get("partitionColumns"):
                print(f"⚠️ Table {table_name} is not partitioned. Consider rebuilding with partitionBy{partition_cols}.")
        except Exception:
            pass
        replace_where = f"{partition_col} >= '{start_dt.strftime('%Y-%m-%d')}' AND {partition_col} <= '{end_dt.strftime('%Y-%m-%d')}'"
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .saveAsTable(table_name)

# Write only the selected date range partition(s)
write_partitioned_table(table_name, events_to_save, start_date, end_date)

# Add comment
spark.sql(f"COMMENT ON TABLE {table_name} IS 'Historical news from GDELT filtered for portfolio stocks (configurable date range)'")

total_records = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
print(f"""
✅ Saved to {table_name}
   New events: {events_final.count()}
   Total in table: {total_records}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save GDELT GKG (Optional)

# COMMAND ----------

if include_gkg:
    if len(gkg_events) == 0:
        print("⚠️  No GKG records found for this date range.")
    else:
        gkg_df = spark.createDataFrame(pd.DataFrame(gkg_events))

        # Explode matched_symbols array so each record-symbol pair is a row
        gkg_exploded = gkg_df.withColumn("symbol", F.explode(F.col("matched_symbols"))) \
            .drop("matched_symbols")

        # Add company info
        gkg_with_company = gkg_exploded.join(
            symbols_df,
            on="symbol",
            how="left"
        )

        # Convert event_date to proper date and cast all columns to correct types
        # Use try_to_date; fall back to source_file_date when event_date is invalid
        gkg_final = gkg_with_company \
            .withColumn("event_date_parsed", F.expr("try_to_date(substr(CAST(event_date AS STRING), 1, 8), 'yyyyMMdd')")) \
            .withColumn("gkg_record_date_parsed", F.expr("try_to_date(substr(CAST(gkg_record_id AS STRING), 1, 8), 'yyyyMMdd')")) \
            .withColumn("source_file_date_parsed", F.expr("try_to_date(substr(CAST(source_file_date AS STRING), 1, 8), 'yyyyMMdd')")) \
            .withColumn(
                "event_date",
                F.coalesce(
                    F.col("event_date_parsed"),
                    F.col("gkg_record_date_parsed"),
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

        gkg_table = f"{catalog}.bronze.historical_news_gdelt_gkg"

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

        write_partitioned_table(gkg_table, gkg_to_save, start_date, end_date)

        spark.sql(f"COMMENT ON TABLE {gkg_table} IS 'Historical GDELT GKG records filtered for portfolio stocks (configurable date range)'")

        gkg_total = spark.sql(f"SELECT COUNT(*) as count FROM {gkg_table}").collect()[0]['count']
        print(f"""
✅ Saved GKG to {gkg_table}
   Total GKG records: {gkg_total}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Events by symbol
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   COUNT(*) as event_count,
# MAGIC   AVG(avg_tone) as avg_sentiment,
# MAGIC   MIN(event_date) as first_event,
# MAGIC   MAX(event_date) as latest_event
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC GROUP BY symbol, company_name
# MAGIC ORDER BY event_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment distribution
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN avg_tone < -5 THEN 'Very Negative'
# MAGIC     WHEN avg_tone < -2 THEN 'Negative'
# MAGIC     WHEN avg_tone < 2 THEN 'Neutral'
# MAGIC     WHEN avg_tone < 5 THEN 'Positive'
# MAGIC     ELSE 'Very Positive'
# MAGIC   END as sentiment_bucket,
# MAGIC   COUNT(*) as event_count,
# MAGIC   AVG(num_articles) as avg_articles
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC GROUP BY sentiment_bucket
# MAGIC ORDER BY 
# MAGIC   CASE sentiment_bucket
# MAGIC     WHEN 'Very Negative' THEN 1
# MAGIC     WHEN 'Negative' THEN 2
# MAGIC     WHEN 'Neutral' THEN 3
# MAGIC     WHEN 'Positive' THEN 4
# MAGIC     WHEN 'Very Positive' THEN 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent high-impact events
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   actor1_name,
# MAGIC   avg_tone,
# MAGIC   num_articles,
# MAGIC   source_url
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE num_articles >= 5
# MAGIC   AND ABS(avg_tone) > 3.0
# MAGIC ORDER BY event_date DESC, num_articles DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Historical Load Complete!
# MAGIC
# MAGIC **This notebook is now configured for a full-year historical load:**
# MAGIC
# MAGIC - **Date range**: Controlled by `target_year` or overrides
# MAGIC - **Total days**: ~365 days for a full year
# MAGIC - **Expected time**: 4-8 hours
# MAGIC - **Expected events**: 500,000 - 2,000,000
# MAGIC
# MAGIC **Note**: If you want to test with a smaller sample first:
# MAGIC 1. Change `start_date = datetime(2024, 12, 1)` for 1 month
# MAGIC 2. Add `[:30]` limit back to the loop
# MAGIC
# MAGIC **This runs unattended** - you can close your browser and check back later!

# COMMAND ----------

print(f"""
================================================================================
✅ GDELT HISTORICAL NEWS INGESTION COMPLETE!
================================================================================

📊 Summary:
   - Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}
   - Events collected: {len(all_events):,}
   - Days processed: {days_processed}
   - Table: riskbricks.bronze.historical_news_gdelt
   
🎯 Data Quality:
   - Stocks covered: Run verification query to count
   - Real historical data from GDELT Project
   - Filtered for your 400+ portfolio stocks
   
🔄 Next Step:
   Run notebooks/03_gold/news/create_news_price_impact.py to calculate:
   - Price before/after each news event
   - 1-day, 1-week, 1-month impact
   - Correlation: news sentiment vs. price movement
   
💡 You now have a full-year set of REAL news-price impact data!
   This enables: "Based on historical events for {start_date.year}..."

================================================================================
""")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'events_collected': len(all_events),
    'days_processed': days_processed,
    'date_range': f'{start_date.year}_full_year',
    'table': 'riskbricks.bronze.historical_news_gdelt'
}))
