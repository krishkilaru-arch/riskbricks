# Databricks notebook source
# MAGIC %md
# MAGIC # 🧾 Build RAG Evidence Log (Gold)
# MAGIC
# MAGIC This creates a daily evidence log for last-N days, used by the
# MAGIC Multi-Agent Supervisor to ground forecasts with verifiable sources.

# COMMAND ----------

# Configuration
from datetime import datetime, timedelta
from pyspark.sql import functions as F

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

days_back = 30
end_date = datetime.utcnow().date()
start_date = end_date - timedelta(days=days_back)

print(f"✅ Evidence window: {start_date} → {end_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Load symbols (scoped)

# COMMAND ----------

symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")

symbols = [r.symbol for r in symbols_df.collect()]
print(f"✅ Symbols: {symbols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Build evidence log from rag_corpus

# COMMAND ----------

rag_df = spark.table("riskbricks.bronze.rag_corpus") \
    .withColumn("as_of_date", F.to_date("published_date")) \
    .filter(F.col("as_of_date").between(F.lit(start_date), F.lit(end_date))) \
    .filter(F.col("symbol").isin(symbols))

evidence_df = rag_df.select(
    F.sha2(F.concat_ws("|",
                       F.col("symbol"),
                       F.col("published_date"),
                       F.coalesce(F.col("url"), F.lit("")),
                       F.coalesce(F.col("title"), F.lit(""))
                       ), 256).alias("evidence_id"),
    F.col("symbol"),
    F.col("as_of_date"),
    F.col("source"),
    F.col("url"),
    F.when(F.col("content").isNotNull(), F.substring(F.col("content"), 1, 500))
     .otherwise(F.substring(F.col("title"), 1, 500)).alias("snippet"),
    (F.length(F.coalesce(F.col("content"), F.col("title"))) / F.lit(1000.0)).alias("score"),
    F.current_timestamp().alias("retrieved_at")
).dropDuplicates(["evidence_id"])

print(f"✅ Evidence rows: {evidence_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Gold (partitioned)

# COMMAND ----------

table_name = f"{catalog}.gold.rag_evidence_log"

if not spark.catalog.tableExists(table_name):
    evidence_df.write \
        .mode("overwrite") \
        .partitionBy("as_of_date", "symbol") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
else:
    replace_where = f"as_of_date >= '{start_date}' AND as_of_date <= '{end_date}'"
    evidence_df.write \
        .mode("overwrite") \
        .option("replaceWhere", replace_where) \
        .saveAsTable(table_name)

spark.sql(f"COMMENT ON TABLE {table_name} IS 'RAG evidence log for last-N days, partitioned by as_of_date and symbol'")

print(f"✅ Saved to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

