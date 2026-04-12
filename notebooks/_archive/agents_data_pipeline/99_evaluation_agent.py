# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ Evaluation Agent (Daily)
# MAGIC
# MAGIC Compares forecasted price to actual close for each model.

# COMMAND ----------

dbutils.widgets.text("symbol", "ALL", "Symbol (ALL for full run)")
dbutils.widgets.text("target_date", "", "Target date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_days", "5", "Lookback days when target_date is empty")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType
from pyspark.sql.window import Window

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

symbol = dbutils.widgets.get("symbol").strip().upper()
target_date = dbutils.widgets.get("target_date").strip()
lookback_days = int(dbutils.widgets.get("lookback_days") or "5")

forecast_tbl = f"{gold_db}.forecast_daily"
price_tbl = f"{gold_db}.stock_prices_daily"
eval_tbl = f"{gold_db}.forecast_eval_daily"

local_tz = ZoneInfo("America/New_York")
if not target_date:
    # Use latest actual date available (per symbol or all).
    latest_actual = spark.table(price_tbl).filter(
        F.col("symbol") == F.lit(symbol) if symbol != "ALL" else F.lit(True)
    ).agg(F.max("date").alias("max_date")).collect()[0]["max_date"]
    if latest_actual is None:
        dbutils.notebook.exit("No actual prices available for evaluation.")
    target_date = latest_actual.strftime("%Y-%m-%d")

if not spark.catalog.tableExists(forecast_tbl):
    raise ValueError(f"Missing forecast table: {forecast_tbl}")
if not spark.catalog.tableExists(price_tbl):
    raise ValueError(f"Missing price table: {price_tbl}")

if target_date:
    start_date = (datetime.strptime(target_date, "%Y-%m-%d").date() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    forecast = spark.table(forecast_tbl).filter(
        (F.col("target_date") >= F.lit(start_date).cast("date")) &
        (F.col("target_date") <= F.lit(target_date).cast("date"))
    )
else:
    forecast = spark.table(forecast_tbl)
if symbol != "ALL":
    forecast = forecast.filter(F.col("symbol") == F.lit(symbol))

window = Window.partitionBy("symbol", "target_date", "method").orderBy(F.col("ingestion_timestamp").desc())
forecast = forecast.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

actual = spark.table(price_tbl).filter(
    (F.col("date") >= F.lit(start_date).cast("date")) &
    (F.col("date") <= F.lit(target_date).cast("date"))
).select(
    F.col("symbol"),
    F.col("date").alias("target_date"),
    F.col("close").alias("actual_close")
)

joined = forecast.join(actual, on=["symbol", "target_date"], how="inner")
forecast_count = forecast.limit(1).count()
actual_count = actual.limit(1).count()
if forecast_count == 0 or actual_count == 0:
    latest_forecast = None
    latest_actual = None
    try:
        latest_forecast = spark.table(forecast_tbl).filter(
            F.col("symbol") == F.lit(symbol) if symbol != "ALL" else F.lit(True)
        ).agg(F.max("target_date").alias("max_date")).collect()[0]["max_date"]
    except Exception:
        latest_forecast = None
    try:
        latest_actual = spark.table(price_tbl).filter(
            F.col("symbol") == F.lit(symbol) if symbol != "ALL" else F.lit(True)
        ).agg(F.max("date").alias("max_date")).collect()[0]["max_date"]
    except Exception:
        latest_actual = None

    print(f"⚠️ Forecast rows for {symbol} in window ending {target_date}: {forecast_count}")
    print(f"⚠️ Actual price rows for {symbol} in window ending {target_date}: {actual_count}")
    print(f"ℹ️ Latest forecast target_date available: {latest_forecast}")
    print(f"ℹ️ Latest actual price date available: {latest_actual}")
    dbutils.notebook.exit("No forecast or actual price available for evaluation.")

eval_df = joined.select(
    F.col("symbol"),
    F.col("target_date").cast("date").alias("target_date"),
    F.col("method"),
    F.col("expected_price").alias("predicted_price"),
    F.col("actual_close").alias("actual_price"),
    (F.col("actual_close") - F.col("expected_price")).alias("error"),
    (F.abs(F.col("actual_close") - F.col("expected_price"))).alias("abs_error"),
    (F.abs(F.col("actual_close") - F.col("expected_price")) / F.col("actual_close")).alias("mape"),
    F.current_timestamp().alias("ingestion_timestamp"),
)

if not spark.catalog.tableExists(eval_tbl):
    eval_df.write.mode("overwrite").partitionBy("target_date", "symbol").saveAsTable(eval_tbl)
else:
    eval_df.createOrReplaceTempView("eval_updates")
    spark.sql(f"""
        MERGE INTO {eval_tbl} t
        USING eval_updates s
        ON t.symbol = s.symbol AND t.target_date = s.target_date AND t.method = s.method
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

row_count = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM {eval_tbl}
    WHERE target_date >= DATE('{start_date}') AND target_date <= DATE('{target_date}')
    {"AND symbol = '" + symbol + "'" if symbol != "ALL" else ""}
""").collect()[0]["c"]
print(f"📊 Validation: forecast_eval_daily rows for {symbol} in window ending {target_date}: {row_count}")

# COMMAND ----------

dbutils.notebook.exit("✅ Evaluation complete")
