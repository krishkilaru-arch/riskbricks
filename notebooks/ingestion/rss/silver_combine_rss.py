# Databricks notebook source
# MAGIC %md
# MAGIC # 🧩 Combine RSS Feeds (Silver)
# MAGIC
# MAGIC Combines Yahoo + Google RSS (and optional Finnhub market news) into a single silver table.

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze RSS Tables

# COMMAND ----------

rss_all_tbl = "riskbricks.bronze.news_rss_all"
if not spark.catalog.tableExists(rss_all_tbl):
    raise ValueError("No RSS tables found in bronze.")

combined = spark.table(rss_all_tbl)

combined = combined.dropDuplicates(["doc_id"]) \
    .withColumn("published_date", F.to_date("published_date")) \
    .withColumn("source", F.coalesce(F.col("source"), F.lit("Unknown")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Silver

# COMMAND ----------

table_name = f"{catalog}.silver.news_rss_combined"

def write_partitioned_table(table_name, df):
    if not spark.catalog.tableExists(table_name):
        df.write \
            .mode("overwrite") \
            .partitionBy("published_date", "symbol") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        min_dt = df.agg(F.min("published_date")).collect()[0][0]
        max_dt = df.agg(F.max("published_date")).collect()[0][0]
        replace_where = f"published_date >= '{min_dt}' AND published_date <= '{max_dt}'"
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .saveAsTable(table_name)

write_partitioned_table(table_name, combined)

print(f"✅ Saved combined RSS to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

