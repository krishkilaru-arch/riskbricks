# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ Evaluate Stock Forecasts
# MAGIC
# MAGIC Compares forecasts to actual closes and logs accuracy.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

# Widgets
dbutils.widgets.text("forecast_date", (datetime.utcnow().date()).strftime("%Y-%m-%d"), "Forecast date (YYYY-MM-DD)")

forecast_date = dbutils.widgets.get("forecast_date").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load forecasts + actuals

# COMMAND ----------

forecasts = spark.table(f"{catalog}.gold.stock_forecasts") \
    .filter(F.col("forecast_date") == F.lit(forecast_date))

if forecasts.count() == 0:
    raise ValueError("No forecasts found for forecast_date.")

price_table = f"{catalog}.silver.stock_prices" if spark.catalog.tableExists(f"{catalog}.silver.stock_prices") else f"{catalog}.bronze.stock_prices_bronze"
prices = spark.table(price_table) \
    .select("symbol", "date", "close") \
    .withColumn("date", F.to_date("date"))

# Join on forecast_date + horizon_days
eval_df = forecasts \
    .withColumn("target_date", F.date_add("forecast_date", F.col("horizon_days"))) \
    .join(prices, (prices.symbol == forecasts.symbol) & (prices.date == F.col("target_date")), "left") \
    .select(
        forecasts["*"],
        prices["close"].alias("actual_price")
    )

eval_df = eval_df \
    .withColumn("error_pct", (F.col("actual_price") - F.col("predicted_price")) / F.col("actual_price")) \
    .withColumn("direction_hit", F.when(
        (F.col("predicted_price") - F.col("last_close")) * (F.col("actual_price") - F.col("last_close")) >= 0,
        F.lit(True)
    ).otherwise(F.lit(False))) \
    .withColumn("evaluated_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Gold

# COMMAND ----------

table_name = f"{catalog}.gold.stock_forecast_eval"

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

write_partitioned_table(table_name, eval_df, forecast_date)

print(f"✅ Saved evaluation to {table_name} for {forecast_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

