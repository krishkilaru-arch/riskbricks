# Databricks notebook source
# MAGIC %md
# MAGIC # 🏅 Merge RSS into Gold RAG Corpus
# MAGIC
# MAGIC Loads combined RSS from Silver and merges into a Gold `rag_corpus` table
# MAGIC so queries can target Google/Yahoo by date and symbol.

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Combined RSS

# COMMAND ----------

source_tbl = f"{catalog}.silver.news_rss_combined"
if not spark.catalog.tableExists(source_tbl):
    raise ValueError(f"Missing source table: {source_tbl}")

rss_df = spark.table(source_tbl).dropDuplicates(["doc_id"])

# Ensure consistent types for downstream queryability
rss_df = rss_df \
    .withColumn("published_date", F.to_date("published_date")) \
    .withColumn("doc_type", F.coalesce(F.col("doc_type"), F.lit("news")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge into Gold rag_corpus

# COMMAND ----------

target_tbl = f"{catalog}.gold.rag_corpus"

if not spark.catalog.tableExists(target_tbl):
    rss_df.write \
        .mode("overwrite") \
        .partitionBy("published_date", "symbol") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_tbl)
else:
    rss_df.createOrReplaceTempView("rss_updates")
    spark.sql(f"""
        MERGE INTO {target_tbl} t
        USING rss_updates s
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

