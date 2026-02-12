# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Train Forecast Model (ML baseline)
# MAGIC
# MAGIC Trains a simple regression model to predict next‑day return
# MAGIC using the `silver.forecast_features_daily` table.

# COMMAND ----------

from datetime import datetime
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

# Widgets
dbutils.widgets.text("start_date", "2025-01-01", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", datetime.utcnow().date().strftime("%Y-%m-%d"), "End date (YYYY-MM-DD)")

start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load feature table and build target

# COMMAND ----------

features = spark.table("riskbricks.silver.forecast_features_daily") \
    .filter((F.col("as_of_date") >= F.lit(start_date)) & (F.col("as_of_date") <= F.lit(end_date))) \
    .select(
        "symbol",
        "as_of_date",
        "last_close",
        "return_5d",
        "return_20d",
        "volatility_20d",
        "event_count_7d",
        "event_count_30d",
        "avg_sentiment_7d",
        "avg_sentiment_30d",
        "evidence_count_30d"
    )

# Next‑day return target from prices
prices = spark.table("riskbricks.silver.stock_prices") \
    .select("symbol", "date", "close") \
    .withColumnRenamed("date", "as_of_date") \
    .withColumn("next_close", F.lead("close").over(Window.partitionBy("symbol").orderBy("as_of_date")))

dataset = features.join(prices.select("symbol", "as_of_date", "next_close"), ["symbol", "as_of_date"], "left") \
    .withColumn("target_return_1d", (F.col("next_close") / F.col("last_close")) - F.lit(1.0)) \
    .dropna(subset=["target_return_1d"])

pdf = dataset.toPandas()

feature_cols = [
    "return_5d", "return_20d", "volatility_20d",
    "event_count_7d", "event_count_30d",
    "avg_sentiment_7d", "avg_sentiment_30d",
    "evidence_count_30d"
]

X = pdf[feature_cols].fillna(0.0)
y = pdf["target_return_1d"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train + Log (MLflow)

# COMMAND ----------

mlflow.set_experiment("/Shared/RiskBricks/forecasting")

with mlflow.start_run(run_name="ridge_forecast_baseline"):
    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = mean_squared_error(y_test, preds, squared=False)

    mlflow.log_param("model", "Ridge")
    mlflow.log_param("alpha", 1.0)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("rmse", rmse)
    # notebook path isn't available here; log metrics + model only

    mlflow.sklearn.log_model(model, "model")

print(f"✅ Trained Ridge model | MAE={mae:.6f}, RMSE={rmse:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

