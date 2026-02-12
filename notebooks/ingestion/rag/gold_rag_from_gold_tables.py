# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 Build RAG Corpus from Gold Tables
# MAGIC
# MAGIC **Purpose**: Denormalize Gold tables into RAG-ready documents and merge into `riskbricks.gold.rag_corpus`.
# MAGIC
# MAGIC **Sources**
# MAGIC - `riskbricks.gold.stock_prices_daily`
# MAGIC - `riskbricks.gold.stock_prices_intraday`
# MAGIC - `riskbricks.gold.macro_indicators_daily`
# MAGIC
# MAGIC **Target**
# MAGIC - `riskbricks.gold.rag_corpus`

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD)")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

local_tz = ZoneInfo("America/New_York")
start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()

def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

if not start_date or not end_date:
    yesterday = datetime.now(local_tz).date() - timedelta(days=1)
    start_date = start_date or yesterday.strftime("%Y-%m-%d")
    end_date = end_date or yesterday.strftime("%Y-%m-%d")
    print(f"ℹ️ Defaulting to yesterday ET: {start_date}")

if _parse_date(end_date) < _parse_date(start_date):
    raise ValueError("end_date must be on or after start_date.")

print(f"📅 Date range (ET): {start_date} → {end_date}")

# COMMAND ----------

target_tbl = f"{gold_db}.rag_corpus"

def _merge_into_rag(df):
    if df is None or df.rdd.isEmpty():
        return
    if not spark.catalog.tableExists(target_tbl):
        df.write \
            .mode("overwrite") \
            .partitionBy("published_date", "symbol") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_tbl)
    else:
        df.createOrReplaceTempView("rag_updates")
        spark.sql(f"""
            MERGE INTO {target_tbl} t
            USING rag_updates s
            ON t.doc_id = s.doc_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

# COMMAND ----------

# 1) Stock Prices Daily
stock_daily_tbl = f"{gold_db}.stock_prices_daily"
stock_daily_docs = None
if spark.catalog.tableExists(stock_daily_tbl):
    stock_daily = spark.table(stock_daily_tbl).filter(
        (F.col("date") >= F.lit(start_date).cast("date")) &
        (F.col("date") <= F.lit(end_date).cast("date"))
    )
    stock_daily_docs = stock_daily.select(
        F.md5(F.concat_ws("::", F.lit("stock_daily"), F.col("symbol"), F.col("date"))).alias("doc_id"),
        F.col("symbol"),
        F.col("symbol").alias("company_name"),
        F.lit("Equity").alias("sector"),
        F.lit("stock_price_daily").alias("doc_type"),
        F.concat(F.lit("Daily price for "), F.col("symbol"), F.lit(" on "), F.col("date")).alias("title"),
        F.concat(
            F.lit("Daily OHLC for "), F.col("symbol"), F.lit(" on "), F.col("date"), F.lit(". "),
            F.lit("Open="), F.col("open"), F.lit(", High="), F.col("high"),
            F.lit(", Low="), F.col("low"), F.lit(", Close="), F.col("close"),
            F.lit(", AdjClose="), F.col("adj_close"), F.lit(", Volume="), F.col("volume")
        ).alias("content"),
        F.lit("Gold Stock Daily").alias("source"),
        F.lit("").alias("url"),
        F.col("date").cast("date").alias("published_date"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

# 2) Stock Prices Intraday (latest hourly bars)
stock_intraday_tbl = f"{gold_db}.stock_prices_intraday"
stock_intraday_docs = None
if spark.catalog.tableExists(stock_intraday_tbl):
    stock_intraday = spark.table(stock_intraday_tbl).filter(
        (F.col("event_date") >= F.lit(start_date).cast("date")) &
        (F.col("event_date") <= F.lit(end_date).cast("date"))
    )
    stock_intraday_docs = stock_intraday.select(
        F.md5(F.concat_ws("::", F.lit("stock_intraday"), F.col("symbol"), F.col("event_ts"))).alias("doc_id"),
        F.col("symbol"),
        F.col("symbol").alias("company_name"),
        F.lit("Equity").alias("sector"),
        F.lit("stock_price_intraday").alias("doc_type"),
        F.concat(F.lit("Intraday price for "), F.col("symbol"), F.lit(" at "), F.col("event_ts")).alias("title"),
        F.concat(
            F.lit("Intraday bar for "), F.col("symbol"), F.lit(" at "), F.col("event_ts"), F.lit(". "),
            F.lit("Open="), F.col("open"), F.lit(", High="), F.col("high"),
            F.lit(", Low="), F.col("low"), F.lit(", Close="), F.col("close"),
            F.lit(", AdjClose="), F.col("adj_close"), F.lit(", Volume="), F.col("volume")
        ).alias("content"),
        F.lit("Gold Stock Intraday").alias("source"),
        F.lit("").alias("url"),
        F.col("event_date").cast("date").alias("published_date"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

# 3) Macro Indicators Daily
macro_tbl = f"{gold_db}.macro_indicators_daily"
macro_docs = None
if spark.catalog.tableExists(macro_tbl):
    macro = spark.table(macro_tbl).filter(
        (F.col("date") >= F.lit(start_date).cast("date")) &
        (F.col("date") <= F.lit(end_date).cast("date"))
    )
    macro_docs = macro.select(
        F.md5(F.concat_ws("::", F.lit("macro"), F.col("indicator_name"), F.col("date"))).alias("doc_id"),
        F.lit("MACRO").alias("symbol"),
        F.col("indicator_name").alias("company_name"),
        F.lit("Macro").alias("sector"),
        F.lit("macro_indicator").alias("doc_type"),
        F.concat(F.lit("Macro indicator "), F.col("indicator_name"), F.lit(" on "), F.col("date")).alias("title"),
        F.concat(
            F.lit("Indicator "), F.col("indicator_name"), F.lit(" on "), F.col("date"), F.lit(". "),
            F.lit("Value="), F.col("value"), F.lit(". "),
            F.lit("Units="), F.coalesce(F.col("units"), F.lit("")),
            F.lit("; Frequency="), F.coalesce(F.col("frequency"), F.lit("")),
            F.lit("; SeasonalAdj="), F.coalesce(F.col("seasonal_adjustment"), F.lit(""))
        ).alias("content"),
        F.lit("Gold Macro").alias("source"),
        F.lit("").alias("url"),
        F.col("date").cast("date").alias("published_date"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

# COMMAND ----------

docs = None
for df in [stock_daily_docs, stock_intraday_docs, macro_docs]:
    if df is not None:
        docs = df if docs is None else docs.unionByName(df)

_merge_into_rag(docs)

if spark.catalog.tableExists(target_tbl):
    total = spark.sql(f"SELECT COUNT(*) as count FROM {target_tbl}").collect()[0]["count"]
    print(f"✅ Gold rag_corpus updated: {total} rows")
else:
    print("⚠️ No documents to write.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

