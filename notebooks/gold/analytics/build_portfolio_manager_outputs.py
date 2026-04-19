# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Portfolio Manager Outputs (Gold)
# MAGIC
# MAGIC Builds:
# MAGIC - `gold.accuracy_scoreboard_daily`
# MAGIC - `gold.decision_signals`
# MAGIC - `gold.risk_adjusted_view`
# MAGIC - `gold.scenario_tests`

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD)")
dbutils.widgets.text("window_days", "30", "Scoreboard window days")
dbutils.widgets.text("decision_threshold", "0.01", "Decision threshold (return)")
dbutils.widgets.text("shock_mkt", "0.00", "Scenario shock: market")
dbutils.widgets.text("shock_smb", "0.00", "Scenario shock: SMB")
dbutils.widgets.text("shock_hml", "0.00", "Scenario shock: HML")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

local_tz = ZoneInfo("America/New_York")
start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
window_days = int(dbutils.widgets.get("window_days") or "30")
decision_threshold = float(dbutils.widgets.get("decision_threshold") or "0.01")
shock_mkt = float(dbutils.widgets.get("shock_mkt") or "0.0")
shock_smb = float(dbutils.widgets.get("shock_smb") or "0.0")
shock_hml = float(dbutils.widgets.get("shock_hml") or "0.0")

if not start_date or not end_date:
    yesterday = datetime.now(local_tz).date() - timedelta(days=1)
    start_date = start_date or yesterday.strftime("%Y-%m-%d")
    end_date = end_date or yesterday.strftime("%Y-%m-%d")

if datetime.strptime(end_date, "%Y-%m-%d") < datetime.strptime(start_date, "%Y-%m-%d"):
    raise ValueError("end_date must be on or after start_date.")

as_of_date = end_date
window_start = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=window_days)).strftime("%Y-%m-%d")

print(f"📅 Date range: {start_date} → {end_date}")
print(f"📅 Scoreboard window: {window_start} → {end_date}")
print(f"✅ Decision threshold: {decision_threshold}")

# COMMAND ----------

def write_partitioned_table(table_name, df, partition_cols, date_col, start_dt, end_dt):
    df = df.filter(
        (F.col(date_col) >= F.lit(start_dt).cast("date")) &
        (F.col(date_col) <= F.lit(end_dt).cast("date"))
    )
    if not spark.catalog.tableExists(table_name):
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        replace_where = f"{date_col} >= '{start_dt}' AND {date_col} <= '{end_dt}'"
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

# COMMAND ----------

# 1) Accuracy Scoreboard
eval_tbl = f"{gold_db}.stock_forecast_eval"
fallback_eval_tbl = f"{gold_db}.forecast_eval"

eval_df = None
if spark.catalog.tableExists(eval_tbl):
    eval_df = spark.table(eval_tbl)
elif spark.catalog.tableExists(fallback_eval_tbl):
    eval_df = spark.table(fallback_eval_tbl)
else:
    print("⚠️ No forecast evaluation table found.")

if eval_df is not None:
    eval_df = eval_df.filter(
        (F.col("forecast_date") >= F.lit(window_start).cast("date")) &
        (F.col("forecast_date") <= F.lit(end_date).cast("date"))
    )

    scoreboard = eval_df.groupBy("symbol", "horizon_days").agg(
        F.count("*").alias("sample_size"),
        F.avg(F.abs(F.col("error_pct"))).alias("mape"),
        F.avg(F.abs(F.col("actual_price") - F.col("predicted_price"))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col("actual_price") - F.col("predicted_price"), 2))).alias("rmse"),
        F.avg(F.when(F.col("direction_hit") == True, 1).otherwise(0)).alias("hit_rate"),
    ).withColumn("window_start", F.lit(window_start).cast("date")) \
     .withColumn("window_end", F.lit(end_date).cast("date")) \
     .withColumn("computed_at", F.current_timestamp())

    scoreboard_tbl = f"{gold_db}.accuracy_scoreboard_daily"
    write_partitioned_table(scoreboard_tbl, scoreboard, ["window_end", "symbol"], "window_end", end_date, end_date)
    print(f"✅ Saved accuracy_scoreboard_daily: {scoreboard_tbl}")

# COMMAND ----------

# 2) Decision Signals
forecast_tbl = f"{gold_db}.stock_forecasts"
price_tbl = f"{catalog}.silver.stock_prices"

if spark.catalog.tableExists(forecast_tbl) and spark.catalog.tableExists(price_tbl):
    forecasts = spark.table(forecast_tbl).filter(
        (F.col("forecast_date") >= F.lit(start_date).cast("date")) &
        (F.col("forecast_date") <= F.lit(end_date).cast("date"))
    )
    prices = spark.table(price_tbl).select(
        F.col("symbol"),
        F.col("date").alias("forecast_date"),
        F.col("close").alias("price_close"),
    )

    signals = forecasts.join(prices, ["symbol", "forecast_date"], "left") \
        .withColumn("last_close", F.coalesce(F.col("last_close"), F.col("price_close"))) \
        .drop("price_close") \
        .withColumn("expected_return", (F.col("predicted_price") - F.col("last_close")) / F.col("last_close")) \
        .withColumn(
            "decision",
            F.when(F.col("expected_return") >= F.lit(decision_threshold), F.lit("BUY"))
             .when(F.col("expected_return") <= F.lit(-decision_threshold), F.lit("SELL"))
             .otherwise(F.lit("HOLD"))
        ) \
        .withColumn("confidence_band_width", F.col("confidence_band_high") - F.col("confidence_band_low")) \
        .withColumn("computed_at", F.current_timestamp())

    signals_tbl = f"{gold_db}.decision_signals"
    write_partitioned_table(signals_tbl, signals, ["forecast_date", "symbol"], "forecast_date", start_date, end_date)
    print(f"✅ Saved decision_signals: {signals_tbl}")
else:
    print("⚠️ Missing stock_forecasts or stock_prices for decision_signals.")

# COMMAND ----------

# (Attribution Summary removed — depended on dropped rag_evidence_log table)

# COMMAND ----------

# 4) Risk-Adjusted View (join decision_signals + factor exposures)
risk_tbl = f"{gold_db}.risk_factor_exposures"
risk_view_tbl = f"{gold_db}.risk_adjusted_view"

if spark.catalog.tableExists(risk_tbl) and spark.catalog.tableExists(f"{gold_db}.decision_signals"):
    risk = spark.table(risk_tbl)
    latest_risk = risk.withColumn(
        "rn", F.row_number().over(Window.partitionBy("symbol").orderBy(F.col("computed_at").desc()))
    ).filter(F.col("rn") == 1).drop("rn")

    signals = spark.table(f"{gold_db}.decision_signals").filter(
        (F.col("forecast_date") >= F.lit(start_date).cast("date")) &
        (F.col("forecast_date") <= F.lit(end_date).cast("date"))
    )

    risk_view = signals.join(latest_risk, "symbol", "left") \
        .withColumn("risk_adjusted_return", F.col("expected_return") / F.col("annualized_vol")) \
        .withColumn("computed_at", F.current_timestamp())

    write_partitioned_table(risk_view_tbl, risk_view, ["forecast_date", "symbol"], "forecast_date", start_date, end_date)
    print(f"✅ Saved risk_adjusted_view: {risk_view_tbl}")
else:
    print("⚠️ Missing inputs for risk_adjusted_view.")

# COMMAND ----------

# 5) Scenario Tests (using FF3 betas)
scenario_tbl = f"{gold_db}.scenario_tests"
if spark.catalog.tableExists(risk_tbl) and spark.catalog.tableExists(f"{gold_db}.decision_signals"):
    risk = spark.table(risk_tbl).filter(F.col("model") == F.lit("ff3"))
    latest_risk = risk.withColumn(
        "rn", F.row_number().over(Window.partitionBy("symbol").orderBy(F.col("computed_at").desc()))
    ).filter(F.col("rn") == 1).drop("rn")

    signals = spark.table(f"{gold_db}.decision_signals").filter(
        (F.col("forecast_date") >= F.lit(start_date).cast("date")) &
        (F.col("forecast_date") <= F.lit(end_date).cast("date"))
    )

    scenario = signals.join(latest_risk, "symbol", "left") \
        .withColumn("shock_mkt", F.lit(shock_mkt)) \
        .withColumn("shock_smb", F.lit(shock_smb)) \
        .withColumn("shock_hml", F.lit(shock_hml)) \
        .withColumn(
            "scenario_return",
            F.col("beta_mkt") * F.col("shock_mkt") +
            F.col("beta_smb") * F.col("shock_smb") +
            F.col("beta_hml") * F.col("shock_hml")
        ) \
        .withColumn(
            "scenario_expected_price",
            F.col("last_close") * F.exp(F.col("expected_return") + F.col("scenario_return"))
        ) \
        .withColumn("computed_at", F.current_timestamp())

    write_partitioned_table(scenario_tbl, scenario, ["forecast_date", "symbol"], "forecast_date", start_date, end_date)
    print(f"✅ Saved scenario_tests: {scenario_tbl}")
else:
    print("⚠️ Missing inputs for scenario_tests.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

