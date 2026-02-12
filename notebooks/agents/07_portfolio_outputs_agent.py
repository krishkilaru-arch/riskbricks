# Databricks notebook source
# MAGIC %md
# MAGIC # 📦 Portfolio Outputs Agent
# MAGIC
# MAGIC Builds PM-ready outputs (accuracy scoreboard, decision signals, attribution,
# MAGIC risk-adjusted views, scenario tests).

# COMMAND ----------

dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

local_tz = ZoneInfo("America/New_York")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

dbutils.notebook.run(
    "/Workspace/Shared/RiskBricks/files/notebooks/03_gold/analytics/build_portfolio_manager_outputs",
    0,
    {"as_of_date": as_of_date}
)

# COMMAND ----------

dbutils.notebook.exit("✅ Portfolio outputs agent complete")
