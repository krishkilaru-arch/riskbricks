# Databricks notebook source
# MAGIC %md
# MAGIC # 📌 Decision Agent (Buy/Hold/Sell)
# MAGIC
# MAGIC Builds decision signals from forecasts, risk metrics, news, and alt signals.

# COMMAND ----------

dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_news_days", "7", "News lookback days")
dbutils.widgets.text("signal_threshold", "0.02", "Buy/Sell threshold")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

local_tz = ZoneInfo("America/New_York")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

lookback_news_days = int(dbutils.widgets.get("lookback_news_days") or "7")
signal_threshold = float(dbutils.widgets.get("signal_threshold") or "0.02")

forecast_tbl = f"{gold_db}.forecast_daily"
risk_tbl = f"{gold_db}.risk_metrics_daily"
rag_tbl = f"{gold_db}.rag_corpus"
earn_tbl = f"{gold_db}.earnings_calendar"
analyst_tbl = f"{gold_db}.analyst_recommendations"
options_tbl = f"{gold_db}.options_iv_skew_daily"
short_tbl = f"{gold_db}.short_interest_snapshot"

for t in (forecast_tbl, risk_tbl):
    if not spark.catalog.tableExists(t):
        raise ValueError(f"Missing table: {t}")

# Latest forecast target_date per symbol
forecast_df = spark.table(forecast_tbl)
latest_target = forecast_df.groupBy("symbol").agg(F.max("target_date").alias("target_date"))
forecast_df = forecast_df.join(latest_target, on=["symbol", "target_date"], how="inner")

# Aggregate forecast across models
forecast_agg = forecast_df.groupBy("symbol", "target_date").agg(
    F.avg("expected_price").alias("expected_price"),
    F.avg("last_price").alias("last_price"),
    F.countDistinct("method").alias("model_count"),
)

# Risk metrics (latest as_of_date per symbol)
risk_df = spark.table(risk_tbl)
latest_risk = risk_df.groupBy("symbol").agg(F.max("as_of_date").alias("as_of_date"))
risk_df = risk_df.join(latest_risk, on=["symbol", "as_of_date"], how="inner")

# News counts
start_news = (datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=lookback_news_days)).strftime("%Y-%m-%d")
news_df = None
if spark.catalog.tableExists(rag_tbl):
    news_df = spark.table(rag_tbl).filter(
        (F.col("published_date") >= F.lit(start_news).cast("date")) &
        (F.col("published_date") <= F.lit(as_of_date).cast("date"))
    ).groupBy("symbol").agg(
        F.count("*").alias("news_doc_count"),
        F.countDistinct("source").alias("news_source_count"),
    )

# Alt signals counts
earn_df = None
if spark.catalog.tableExists(earn_tbl):
    earn_df = spark.table(earn_tbl).filter(
        F.col("event_date") >= F.lit(start_news).cast("date")
    ).groupBy("symbol").agg(F.count("*").alias("earnings_count"))

analyst_df = None
if spark.catalog.tableExists(analyst_tbl):
    analyst_df = spark.table(analyst_tbl).filter(
        F.col("event_date") >= F.lit(start_news).cast("date")
    ).groupBy("symbol").agg(F.count("*").alias("analyst_count"))

options_df = None
if spark.catalog.tableExists(options_tbl):
    options_df = spark.table(options_tbl)
    latest_opt = options_df.groupBy("symbol").agg(F.max("as_of_date").alias("as_of_date"))
    options_df = options_df.join(latest_opt, on=["symbol", "as_of_date"], how="inner") \
        .select("symbol", F.col("iv_skew").alias("options_iv_skew"))

short_df = None
if spark.catalog.tableExists(short_tbl):
    short_df = spark.table(short_tbl)
    latest_short = short_df.groupBy("symbol").agg(F.max("as_of_date").alias("as_of_date"))
    short_df = short_df.join(latest_short, on=["symbol", "as_of_date"], how="inner") \
        .select("symbol", F.col("short_ratio").alias("short_ratio"))

df = forecast_agg.join(risk_df.select("symbol", "vol_20d", "beta_1y"), on="symbol", how="left")
if news_df is not None:
    df = df.join(news_df, on="symbol", how="left")
if earn_df is not None:
    df = df.join(earn_df, on="symbol", how="left")
if analyst_df is not None:
    df = df.join(analyst_df, on="symbol", how="left")
if options_df is not None:
    df = df.join(options_df, on="symbol", how="left")
if short_df is not None:
    df = df.join(short_df, on="symbol", how="left")

df = df.fillna(0.0)
df = df.withColumn("expected_return", (F.col("expected_price") / F.col("last_price")) - F.lit(1.0))
df = df.withColumn(
    "score",
    F.col("expected_return")
    - 0.5 * F.col("vol_20d")
    + 0.02 * F.col("news_doc_count")
    + 0.05 * F.col("analyst_count")
    + 0.05 * F.col("earnings_count")
    - 0.05 * F.col("options_iv_skew")
    - 0.02 * F.col("short_ratio")
)

df = df.withColumn(
    "signal",
    F.when(F.col("score") >= F.lit(signal_threshold), F.lit("BUY"))
     .when(F.col("score") <= F.lit(-signal_threshold), F.lit("SELL"))
     .otherwise(F.lit("HOLD"))
)

output_tbl = f"{gold_db}.decision_signals"
final_df = df.select(
    F.col("symbol"),
    F.lit(as_of_date).cast("date").alias("as_of_date"),
    "target_date",
    "signal",
    "score",
    "expected_return",
    "model_count",
    "vol_20d",
    "beta_1y",
    "news_doc_count",
    "news_source_count",
    "earnings_count",
    "analyst_count",
    "options_iv_skew",
    "short_ratio",
    F.current_timestamp().alias("ingestion_timestamp"),
)

if not spark.catalog.tableExists(output_tbl):
    final_df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(output_tbl)
else:
    # Deduplicate to prevent MERGE conflicts
    # Keep the latest record for each (symbol, as_of_date, target_date)
    from pyspark.sql.window import Window
    
    final_df_deduped = final_df \
        .withColumn("row_num", F.row_number().over(
            Window.partitionBy("symbol", "as_of_date", "target_date")
                  .orderBy(F.col("ingestion_timestamp").desc())
        )) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    final_df_deduped.createOrReplaceTempView("decision_updates")
    spark.sql(f"""
        MERGE INTO {output_tbl} t
        USING decision_updates s
        ON t.symbol = s.symbol AND t.as_of_date = s.as_of_date AND t.target_date = s.target_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"✅ Decision signals saved to {output_tbl}")

# COMMAND ----------

dbutils.notebook.exit("✅ Decision agent complete")
