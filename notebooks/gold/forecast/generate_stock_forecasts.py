# Databricks notebook source
# MAGIC %md
# MAGIC # 🔮 Generate Stock Forecasts
# MAGIC
# MAGIC Creates 1‑day and 15‑day forecasts from daily features.

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

# Widgets
dbutils.widgets.text("as_of_date", (datetime.utcnow().date()).strftime("%Y-%m-%d"), "As of date (YYYY-MM-DD)")
dbutils.widgets.text("sentiment_weight", "0.30", "Sentiment weight")
dbutils.widgets.text("momentum_weight", "0.70", "Momentum weight")

as_of_date = dbutils.widgets.get("as_of_date").strip()
sentiment_weight = float(dbutils.widgets.get("sentiment_weight"))
momentum_weight = float(dbutils.widgets.get("momentum_weight"))

print(f"✅ Forecast date: {as_of_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load features

# COMMAND ----------

allowed_symbols = spark.table(f"{catalog}.gold.company_universe") \
    .select("symbol").distinct()

features = spark.table(f"{catalog}.silver.forecast_features_daily") \
    .filter(F.col("as_of_date") == F.lit(as_of_date)) \
    .join(allowed_symbols, "symbol", "inner")

if features.count() == 0:
    raise ValueError("No features found for as_of_date. Run build_forecast_features_daily first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build forecasts

# COMMAND ----------

sentiment_signal_7d = F.col("avg_sentiment_7d") / F.lit(10.0)
sentiment_signal_30d = F.col("avg_sentiment_30d") / F.lit(10.0)
return_5d = F.coalesce(F.col("return_5d"), F.lit(0.0))
return_20d = F.coalesce(F.col("return_20d"), F.col("return_5d"), F.lit(0.0))

pred_return_1d = (sentiment_weight * sentiment_signal_7d) + (momentum_weight * return_5d)
pred_return_15d = (sentiment_weight * sentiment_signal_30d) + (momentum_weight * return_20d)

forecast_1d = features \
    .withColumn("forecast_date", F.lit(as_of_date).cast("date")) \
    .withColumn("horizon_days", F.lit(1)) \
    .withColumn("predicted_price", F.col("last_close") * (F.lit(1.0) + pred_return_1d)) \
    .withColumn("predicted_direction", F.when(pred_return_1d >= 0, F.lit("up")).otherwise(F.lit("down"))) \
    .withColumn("confidence_band_low", F.col("last_close") * (F.lit(1.0) - F.coalesce(F.col("volatility_20d"), F.lit(0.02)))) \
    .withColumn("confidence_band_high", F.col("last_close") * (F.lit(1.0) + F.coalesce(F.col("volatility_20d"), F.lit(0.02))))

forecast_15d = features \
    .withColumn("forecast_date", F.lit(as_of_date).cast("date")) \
    .withColumn("horizon_days", F.lit(15)) \
    .withColumn("predicted_price", F.col("last_close") * (F.lit(1.0) + pred_return_15d)) \
    .withColumn("predicted_direction", F.when(pred_return_15d >= 0, F.lit("up")).otherwise(F.lit("down"))) \
    .withColumn("confidence_band_low", F.col("last_close") * (F.lit(1.0) - F.coalesce(F.col("volatility_20d"), F.lit(0.02)))) \
    .withColumn("confidence_band_high", F.col("last_close") * (F.lit(1.0) + F.coalesce(F.col("volatility_20d"), F.lit(0.02))))

def build_top_factors():
    return F.array(
        F.concat(F.lit("sentiment_7d="), F.round(F.col("avg_sentiment_7d"), 2)),
        F.concat(F.lit("return_5d="), F.round(F.col("return_5d"), 4)),
        F.concat(F.lit("volatility_20d="), F.round(F.col("volatility_20d"), 4)),
        F.concat(F.lit("event_count_30d="), F.col("event_count_30d"))
    )

def build_snapshot():
    return F.to_json(F.struct(
        "last_close", "return_5d", "return_20d", "volatility_20d",
        "event_count_7d", "event_count_30d",
        "avg_sentiment_7d", "avg_sentiment_30d",
        "evidence_count_30d"
    ))

forecast_1d = forecast_1d.withColumn("top_factors", build_top_factors()) \
    .withColumn("feature_snapshot", build_snapshot())
forecast_15d = forecast_15d.withColumn("top_factors", build_top_factors()) \
    .withColumn("feature_snapshot", build_snapshot())

forecasts = forecast_1d.unionByName(forecast_15d)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Gold

# COMMAND ----------

table_name = f"{catalog}.gold.stock_forecasts"

def write_partitioned_table(table_name, df, forecast_dt):
    if not spark.catalog.tableExists(table_name):
        df.write \
            .mode("overwrite") \
            .partitionBy("forecast_date", "symbol") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        replace_where = f"forecast_date = '{forecast_dt}'"
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .saveAsTable(table_name)

write_partitioned_table(table_name, forecasts, as_of_date)

print(f"✅ Saved forecasts to {table_name} for {as_of_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

