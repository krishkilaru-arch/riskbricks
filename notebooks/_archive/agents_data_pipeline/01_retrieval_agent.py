# Databricks notebook source
# MAGIC %md
# MAGIC # 🔎 Retrieval Agent (RAG Evidence)
# MAGIC
# MAGIC Pulls recent RAG docs for a symbol and writes evidence log.

# COMMAND ----------

dbutils.widgets.text("symbol", "NVDA", "Symbol (or ALL)")
dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_days", "30", "Lookback days")
dbutils.widgets.text("max_symbols", "0", "Max symbols when ALL (0 = all)")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

symbol = dbutils.widgets.get("symbol").strip().upper()
as_of_date = dbutils.widgets.get("as_of_date").strip()
lookback_days = int(dbutils.widgets.get("lookback_days") or "30")
max_symbols = int(dbutils.widgets.get("max_symbols") or "0")

local_tz = ZoneInfo("America/New_York")
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

start_date = (datetime.strptime(as_of_date, "%Y-%m-%d").date() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

print(f"✅ Symbol: {symbol}")
print(f"✅ As of date: {as_of_date}")
print(f"✅ Lookback start: {start_date}")

# COMMAND ----------

rag_tbl = f"{gold_db}.rag_corpus"
if not spark.catalog.tableExists(rag_tbl):
    raise ValueError(f"Missing table: {rag_tbl}")

evidence_tbl = f"{gold_db}.rag_evidence_log"
metrics_tbl = f"{gold_db}.rag_retrieval_metrics_daily"

def _table_has_columns(table_name, required_cols):
    if not spark.catalog.tableExists(table_name):
        return False
    existing = {c.name for c in spark.catalog.listColumns(table_name)}
    return all(col in existing for col in required_cols)

def process_symbol(sym):
    docs = spark.table(rag_tbl).filter(
        (F.col("symbol") == F.lit(sym)) &
        (F.col("published_date") >= F.lit(start_date).cast("date")) &
        (F.col("published_date") <= F.lit(as_of_date).cast("date"))
    )
    evidence_count = docs.count()
    print(f"✅ Evidence rows for {sym}: {evidence_count}")

    metrics_schema = StructType([
        StructField("as_of_date", DateType(), False),
        StructField("symbol", StringType(), False),
        StructField("lookback_days", IntegerType(), False),
        StructField("doc_count", IntegerType(), False),
        StructField("source_count", IntegerType(), False),
        StructField("min_published_date", DateType(), True),
        StructField("max_published_date", DateType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ])

    metrics_row = docs.agg(
        F.lit(as_of_date).cast("date").alias("as_of_date"),
        F.lit(sym).alias("symbol"),
        F.lit(lookback_days).cast("int").alias("lookback_days"),
        F.count("*").cast("int").alias("doc_count"),
        F.countDistinct("source").cast("int").alias("source_count"),
        F.min("published_date").alias("min_published_date"),
        F.max("published_date").alias("max_published_date"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )

    schema = StructType([
        StructField("as_of_date", DateType(), False),
        StructField("symbol", StringType(), False),
        StructField("doc_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("source", StringType(), True),
        StructField("published_date", DateType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ])

    evidence_df = docs.select(
        F.lit(as_of_date).cast("date").alias("as_of_date"),
        F.col("symbol"),
        F.col("doc_id"),
        F.col("title"),
        F.col("source"),
        F.col("published_date"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )

    required_cols = ["as_of_date", "symbol", "doc_id", "title", "source", "published_date", "ingestion_timestamp"]
    if not spark.catalog.tableExists(evidence_tbl):
        evidence_df.write.mode("overwrite").saveAsTable(evidence_tbl)
    elif not _table_has_columns(evidence_tbl, required_cols):
        print(f"⚠️ Existing {evidence_tbl} schema does not match expected columns. Overwriting with new schema.")
        evidence_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(evidence_tbl)
    else:
        evidence_df.createOrReplaceTempView("evidence_updates")
        spark.sql(f"""
            MERGE INTO {evidence_tbl} t
            USING evidence_updates s
            ON t.symbol = s.symbol AND t.doc_id = s.doc_id AND t.as_of_date = s.as_of_date
            WHEN NOT MATCHED THEN INSERT *
        """)

    if not spark.catalog.tableExists(metrics_tbl):
        metrics_row.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(metrics_tbl)
    else:
        metrics_row.createOrReplaceTempView("metrics_updates")
        spark.sql(f"""
            MERGE INTO {metrics_tbl} t
            USING metrics_updates s
            ON t.symbol = s.symbol AND t.as_of_date = s.as_of_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    evidence_count_out = spark.sql(f"""
        SELECT COUNT(*) AS c
        FROM {evidence_tbl}
        WHERE symbol = '{sym}' AND as_of_date = DATE('{as_of_date}')
    """).collect()[0]["c"]

    metrics_count_out = spark.sql(f"""
        SELECT COUNT(*) AS c
        FROM {metrics_tbl}
        WHERE symbol = '{sym}' AND as_of_date = DATE('{as_of_date}')
    """).collect()[0]["c"]

    print(f"📊 Validation: evidence rows for {sym} on {as_of_date}: {evidence_count_out}")
    print(f"📊 Validation: metrics rows for {sym} on {as_of_date}: {metrics_count_out}")

if symbol == "ALL":
    symbols_df = spark.sql("""
        SELECT DISTINCT symbol
        FROM riskbricks.gold.company_universe
        ORDER BY symbol
    """)
    symbols = [row.symbol for row in symbols_df.collect()]
    if max_symbols and max_symbols > 0:
        symbols = symbols[:max_symbols]
    for sym in symbols:
        process_symbol(sym)
else:
    process_symbol(symbol)

exit_message = f"✅ Retrieval complete for {symbol}"

# COMMAND ----------

dbutils.notebook.exit(exit_message)
