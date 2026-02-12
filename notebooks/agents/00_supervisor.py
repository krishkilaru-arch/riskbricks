# Databricks notebook source
# MAGIC %md
# MAGIC # 🧭 Multi-Agent Supervisor (Orchestrator)
# MAGIC
# MAGIC Orchestrates retrieval, forecasting, risk, and evaluation agents.

# COMMAND ----------

dbutils.widgets.text("symbol", "NVDA", "Symbol (or ALL)")
dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")
dbutils.widgets.text("target_date", "", "Forecast target date (YYYY-MM-DD)")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

symbol = dbutils.widgets.get("symbol").strip().upper()
as_of_date = dbutils.widgets.get("as_of_date").strip()
target_date = dbutils.widgets.get("target_date").strip()

local_tz = ZoneInfo("America/New_York")
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
if not target_date:
    target_date = as_of_date

run_id = str(uuid.uuid4())
print(f"✅ Run ID: {run_id}")
print(f"✅ Symbol: {symbol}")
print(f"✅ As of date: {as_of_date}")
print(f"✅ Target date: {target_date}")

# COMMAND ----------

def log_run(status, details=""):
    table_name = f"{gold_db}.agent_run_log"
    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("as_of_date", StringType(), False),
        StructField("target_date", StringType(), False),
        StructField("status", StringType(), False),
        StructField("details", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ])
    data = [(run_id, symbol, as_of_date, target_date, status, details, datetime.now(local_tz))]
    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

try:
    symbols = [symbol]
    if symbol == "ALL":
        symbols_df = spark.sql("""
            SELECT DISTINCT symbol
            FROM riskbricks.gold.company_universe
            ORDER BY symbol
        """)
        symbols = [row.symbol for row in symbols_df.collect()]

    # 0) News Analytics Agent
    dbutils.notebook.run(
        "/Workspace/Shared/RiskBricks/files/notebooks/agents/05_news_analytics_agent",
        0,
        {"as_of_date": as_of_date}
    )

    for sym in symbols:
        # 1) Retrieval Agent
        dbutils.notebook.run(
            "/Workspace/Shared/RiskBricks/files/notebooks/agents/01_retrieval_agent",
            0,
            {"symbol": sym, "as_of_date": as_of_date}
        )

        # 2) Forecast Agent
        dbutils.notebook.run(
            "/Workspace/Shared/RiskBricks/files/notebooks/agents/02_forecast_agent",
            0,
            {"symbol": sym, "target_date": target_date}
        )

        # 3) Risk Agent
        dbutils.notebook.run(
            "/Workspace/Shared/RiskBricks/files/notebooks/agents/03_risk_agent",
            0,
            {"symbol": sym, "as_of_date": as_of_date}
        )

    # 3b) Factor Exposure Agent (Barra-like)
    dbutils.notebook.run(
        "/Workspace/Shared/RiskBricks/files/notebooks/agents/04_factor_exposure_agent",
        0,
        {"start_date": as_of_date, "end_date": as_of_date}
    )

    # 4) Decision Agent
    dbutils.notebook.run(
        "/Workspace/Shared/RiskBricks/files/notebooks/agents/06_decision_agent",
        0,
        {"as_of_date": as_of_date}
    )

    # 5) Portfolio Outputs Agent
    dbutils.notebook.run(
        "/Workspace/Shared/RiskBricks/files/notebooks/agents/07_portfolio_outputs_agent",
        0,
        {"as_of_date": as_of_date}
    )

    # 6) Evaluation Agent (end)
    dbutils.notebook.run(
        "/Workspace/Shared/RiskBricks/files/notebooks/agents/99_evaluation_agent",
        0,
        {"symbol": "ALL" if symbol == "ALL" else symbol, "target_date": target_date}
    )

    log_run("success")
    dbutils.notebook.exit(json.dumps({"run_id": run_id, "status": "success"}))
except Exception as exc:
    log_run("failed", details=str(exc))
    raise
