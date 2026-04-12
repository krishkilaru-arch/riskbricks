# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 News Analytics Agent
# MAGIC
# MAGIC Runs news impact analysis and geopolitical stress generation.

# COMMAND ----------

dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")

# COMMAND ----------

# --- RiskBricks Config (auto-detect paths) ---
import sys, os
_nb_path = os.path.dirname(dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get())
_repo_root = _nb_path
while _repo_root and not os.path.exists(f"/Workspace{_repo_root}/config/riskbricks_config.py"):
    _repo_root = os.path.dirname(_repo_root)
sys.path.insert(0, f"/Workspace{_repo_root}")
from config.riskbricks_config import cfg
# --- End Config ---


from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

local_tz = ZoneInfo("America/New_York")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

print(f"✅ News analytics as_of_date: {as_of_date}")

# Run historical news impact analysis (GDELT + prices)
dbutils.notebook.run(
    cfg.notebook("03_gold/news/create_news_price_impact"),
    0,
    {}
)

# Run geopolitical stress analysis if sentiment table exists
if spark.catalog.tableExists("riskbricks.silver.news_sentiment"):
    dbutils.notebook.run(
        cfg.notebook("03_gold/news/create_geopolitical_stress"),
        0,
        {}
    )
else:
    print("⚠️ Skipping geopolitical stress: missing riskbricks.silver.news_sentiment")

# COMMAND ----------

dbutils.notebook.exit("✅ News analytics agent complete")
