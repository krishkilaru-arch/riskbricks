# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Build Forecast Features (Daily)
# MAGIC
# MAGIC Generates daily feature rows per symbol for forecasting.

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

# Widgets
dbutils.widgets.text("start_date", (datetime.utcnow().date() - timedelta(days=30)).strftime("%Y-%m-%d"), "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", datetime.utcnow().date().strftime("%Y-%m-%d"), "End date (YYYY-MM-DD)")

start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()

print(f"✅ Feature window: {start_date} → {end_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Load price data (silver if available, else bronze)

# COMMAND ----------

price_table = "riskbricks.silver.stock_prices" if spark.catalog.tableExists("riskbricks.silver.stock_prices") else "riskbricks.bronze.stock_prices_bronze"
prices = spark.table(price_table) \
    .select("symbol", "date", "close") \
    .withColumn("date", F.to_date("date"))

allowed_symbols = spark.table("riskbricks.gold.company_universe") \
    .select("symbol").distinct()

prices = prices.filter(
    (F.col("date") >= F.lit(start_date)) &
    (F.col("date") <= F.lit(end_date))
).join(allowed_symbols, "symbol", "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Price‑based features

# COMMAND ----------

window_by_symbol = Window.partitionBy("symbol").orderBy("date")

prices_feat = prices \
    .withColumn("return_1d", F.col("close") / F.lag("close").over(window_by_symbol) - F.lit(1.0)) \
    .withColumn("return_5d", F.col("close") / F.lag("close", 5).over(window_by_symbol) - F.lit(1.0)) \
    .withColumn("return_20d", F.col("close") / F.lag("close", 20).over(window_by_symbol) - F.lit(1.0)) \
    .withColumn("volatility_20d", F.stddev("return_1d").over(window_by_symbol.rowsBetween(-19, 0))) \
    .select(
        F.col("symbol"),
        F.col("date").alias("as_of_date"),
        F.col("close").alias("last_close"),
        "return_5d",
        "return_20d",
        "volatility_20d"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Event‑based features (GDELT)

# COMMAND ----------

events = spark.table("riskbricks.bronze.historical_news_gdelt") \
    .select("symbol", "event_date", "avg_tone") \
    .withColumn("event_date", F.to_date("event_date"))

events = events.filter(
    (F.col("event_date") >= F.lit(start_date)) &
    (F.col("event_date") <= F.lit(end_date))
).join(allowed_symbols, "symbol", "inner")

event_7d = events.groupBy("symbol", "event_date") \
    .agg(F.avg("avg_tone").alias("daily_sentiment"),
         F.count("*").alias("daily_events"))

event_30d = events.groupBy("symbol", "event_date") \
    .agg(F.avg("avg_tone").alias("daily_sentiment_30d"),
         F.count("*").alias("daily_events_30d"))

# Rolling windows
event_window = Window.partitionBy("symbol").orderBy("event_date").rowsBetween(-6, 0)
event_window_30 = Window.partitionBy("symbol").orderBy("event_date").rowsBetween(-29, 0)

event_features = event_7d \
    .withColumn("event_count_7d", F.sum("daily_events").over(event_window)) \
    .withColumn("avg_sentiment_7d", F.avg("daily_sentiment").over(event_window)) \
    .select(
        "symbol",
        F.col("event_date").alias("as_of_date"),
        "event_count_7d",
        "avg_sentiment_7d"
    )

event_features_30 = event_30d \
    .withColumn("event_count_30d", F.sum("daily_events_30d").over(event_window_30)) \
    .withColumn("avg_sentiment_30d", F.avg("daily_sentiment_30d").over(event_window_30)) \
    .select(
        "symbol",
        F.col("event_date").alias("as_of_date"),
        "event_count_30d",
        "avg_sentiment_30d"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Evidence log features (RAG)

# COMMAND ----------

evidence = spark.table("riskbricks.gold.rag_evidence_log") \
    .select("symbol", "as_of_date", "evidence_id")

evidence = evidence.filter(
    (F.col("as_of_date") >= F.lit(start_date)) &
    (F.col("as_of_date") <= F.lit(end_date))
).join(allowed_symbols, "symbol", "inner")

evidence_features = evidence.groupBy("symbol", "as_of_date") \
    .agg(F.count("*").alias("evidence_count_1d"))

evidence_window_30 = Window.partitionBy("symbol").orderBy("as_of_date").rowsBetween(-29, 0)
evidence_features = evidence_features \
    .withColumn("evidence_count_30d", F.sum("evidence_count_1d").over(evidence_window_30)) \
    .select("symbol", "as_of_date", "evidence_count_30d")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Join features

# COMMAND ----------

features = prices_feat \
    .join(event_features, ["symbol", "as_of_date"], "left") \
    .join(event_features_30, ["symbol", "as_of_date"], "left") \
    .join(evidence_features, ["symbol", "as_of_date"], "left")

features = features.fillna({
    "event_count_7d": 0,
    "avg_sentiment_7d": 0.0,
    "event_count_30d": 0,
    "avg_sentiment_30d": 0.0,
    "evidence_count_30d": 0
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Silver

# COMMAND ----------

table_name = f"{catalog}.silver.forecast_features_daily"

def write_partitioned_table(table_name, df, start_dt, end_dt):
    df = df.filter(
        (F.col("as_of_date") >= F.lit(start_dt).cast("date")) &
        (F.col("as_of_date") <= F.lit(end_dt).cast("date"))
    )
    if not spark.catalog.tableExists(table_name):
        df.write \
            .mode("overwrite") \
            .partitionBy("as_of_date", "symbol") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        replace_where = f"as_of_date >= '{start_dt}' AND as_of_date <= '{end_dt}'"
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .saveAsTable(table_name)

write_partitioned_table(table_name, features, start_date, end_date)

print(f"✅ Saved features to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

