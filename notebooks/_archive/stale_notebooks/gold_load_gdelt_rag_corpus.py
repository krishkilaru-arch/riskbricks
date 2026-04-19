# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 Merge GDELT into Gold RAG Corpus
# MAGIC
# MAGIC **Purpose**: Transform GDELT Events + GKG from Bronze into the unified Gold `rag_corpus`.
# MAGIC
# MAGIC **Sources**:
# MAGIC - `riskbricks.bronze.historical_news_gdelt`
# MAGIC - `riskbricks.bronze.historical_news_gdelt_gkg`
# MAGIC
# MAGIC **Target**:
# MAGIC - `riskbricks.gold.rag_corpus` (partitioned by `published_date`, `symbol`)

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

local_tz = ZoneInfo("America/New_York")
start_date_input = dbutils.widgets.get("start_date").strip()
end_date_input = dbutils.widgets.get("end_date").strip()

def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

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

events_src = f"{catalog}.bronze.historical_news_gdelt"
gkg_src = f"{catalog}.bronze.historical_news_gdelt_gkg"

if not spark.catalog.tableExists(events_src):
    raise ValueError(f"Missing source table: {events_src}")
if not spark.catalog.tableExists(gkg_src):
    raise ValueError(f"Missing source table: {gkg_src}")

events_df = spark.table(events_src).filter(
    (F.col("event_date") >= F.lit(start_date.strftime("%Y-%m-%d")).cast("date")) &
    (F.col("event_date") <= F.lit(end_date.strftime("%Y-%m-%d")).cast("date"))
)

gkg_df = spark.table(gkg_src).filter(
    (F.col("event_date") >= F.lit(start_date.strftime("%Y-%m-%d")).cast("date")) &
    (F.col("event_date") <= F.lit(end_date.strftime("%Y-%m-%d")).cast("date"))
)

# COMMAND ----------

gdelt_docs = events_df.select(
    F.concat(F.lit("gdelt_"), F.col("event_id")).alias("doc_id"),
    F.col("symbol"),
    F.coalesce(F.col("company_name"), F.col("symbol")).alias("company_name"),
    F.coalesce(F.col("sector"), F.lit("Unknown")).alias("sector"),
    F.lit("historical_news").alias("doc_type"),
    F.concat(F.lit("GDELT Event: "), F.coalesce(F.col("actor1_name"), F.lit("Unknown")), F.lit(" - "), F.col("symbol")).alias("title"),
    F.concat(
        F.lit("Historical news event on "), F.col("event_date"), F.lit(". "),
        F.lit("Actors: "), F.coalesce(F.col("actor1_name"), F.lit("Unknown")), F.lit(" and "),
        F.coalesce(F.col("actor2_name"), F.lit("Unknown")), F.lit(". "),
        F.lit("Sentiment: "), F.round(F.col("avg_tone"), 2), F.lit(". "),
        F.lit("Mentions: "), F.col("num_mentions"), F.lit(". "),
        F.lit("Source: "), F.coalesce(F.col("source_url"), F.lit("GDELT Database"))
    ).alias("content"),
    F.lit("GDELT").alias("source"),
    F.coalesce(F.col("source_url"), F.lit("")).alias("url"),
    F.col("event_date").cast("date").alias("published_date"),
    F.current_timestamp().alias("ingestion_timestamp")
)

gkg_docs = gkg_df.select(
    F.concat(F.lit("gkg_"), F.col("gkg_record_id")).alias("doc_id"),
    F.col("symbol"),
    F.coalesce(F.col("company_name"), F.col("symbol")).alias("company_name"),
    F.coalesce(F.col("sector"), F.lit("Unknown")).alias("sector"),
    F.lit("historical_news_gkg").alias("doc_type"),
    F.concat(F.lit("GKG: "), F.coalesce(F.col("source_common_name"), F.lit("Unknown"))).alias("title"),
    F.concat(
        F.lit("GDELT GKG metadata for "), F.col("symbol"), F.lit(". "),
        F.lit("Source: "), F.coalesce(F.col("source_common_name"), F.lit("Unknown")), F.lit(". "),
        F.lit("Themes: "), F.coalesce(F.col("themes"), F.lit("")), F.lit(". "),
        F.lit("Organizations: "), F.coalesce(F.col("organizations"), F.lit("")), F.lit(". "),
        F.lit("Persons: "), F.coalesce(F.col("persons"), F.lit("")), F.lit(". "),
        F.lit("Tone: "), F.coalesce(F.col("tone"), F.lit(""))
    ).alias("content"),
    F.lit("GDELT GKG").alias("source"),
    F.coalesce(F.col("document_identifier"), F.lit("")).alias("url"),
    F.col("event_date").cast("date").alias("published_date"),
    F.current_timestamp().alias("ingestion_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👀 Preview Merged Records

# COMMAND ----------

merged_preview = gdelt_docs.unionByName(gkg_docs)
merged_preview.select(
    "doc_id",
    "symbol",
    "doc_type",
    "title",
    "published_date",
    "source"
).show(30, truncate=80)

# COMMAND ----------

target_tbl = f"{catalog}.gold.rag_corpus"

if not spark.catalog.tableExists(target_tbl):
    merged = gdelt_docs.unionByName(gkg_docs)
    merged.write \
        .mode("overwrite") \
        .partitionBy("published_date", "symbol") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_tbl)
else:
    # Deduplicate source data to prevent MERGE conflicts
    # Keep the latest record for each doc_id
    gdelt_deduped = gdelt_docs \
        .withColumn("row_num", F.row_number().over(
            Window.partitionBy("doc_id").orderBy(F.col("ingestion_timestamp").desc())
        )) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    gkg_deduped = gkg_docs \
        .withColumn("row_num", F.row_number().over(
            Window.partitionBy("doc_id").orderBy(F.col("ingestion_timestamp").desc())
        )) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    gdelt_deduped.createOrReplaceTempView("gdelt_updates")
    spark.sql(f"""
        MERGE INTO {target_tbl} t
        USING gdelt_updates s
        ON t.doc_id = s.doc_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    gkg_deduped.createOrReplaceTempView("gkg_updates")
    spark.sql(f"""
        MERGE INTO {target_tbl} t
        USING gkg_updates s
        ON t.doc_id = s.doc_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

total = spark.sql(f"SELECT COUNT(*) as count FROM {target_tbl}").collect()[0]["count"]
print(f"✅ Gold rag_corpus updated: {total} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

