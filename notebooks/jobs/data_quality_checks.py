# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC Automated data quality validation for all RiskBricks tables.
# MAGIC Runs after daily refresh jobs to verify data integrity.
# MAGIC
# MAGIC **Checks:** Freshness | Row counts | Null rates | Schema | Cross-table consistency

# COMMAND ----------

# ── Import centralized config ────────────────────────────────────────
import sys, os
_nb  = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_root = "/Workspace" + (_nb[:_nb.find("/notebooks/")] if "/notebooks/" in _nb else os.path.dirname(_nb))
sys.path.insert(0, _root)
from config import CATALOG, setup_logger

import json
from datetime import datetime, timedelta, timezone

logger = setup_logger("riskbricks.data_quality")

dbutils.widgets.text("catalog", CATALOG)
dbutils.widgets.text("alert_on_failure", "true")
catalog = dbutils.widgets.get("catalog").strip()

spark.sql(f"USE CATALOG {catalog}")

results = []  # Collect all check results

def check(name, passed, detail=""):
    """Record a DQ check result."""
    status = "PASS" if passed else "FAIL"
    results.append({"check": name, "status": status, "detail": detail})
    icon = "✅" if passed else "❌"
    logger.info(json.dumps({"check": name, "status": status, "detail": detail}))
    print(f"  {icon} {name}: {detail}")

print(f"\n{'='*60}")
print(f"DATA QUALITY CHECKS — {catalog}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table Existence & Row Counts

# COMMAND ----------

print("\n📊 Table Existence & Row Counts")
print("-" * 40)

required_tables = {
    "bronze.stock_prices_bronze": 1000,
    "bronze.fred_macro_indicators": 50,
    "bronze.news_rss_all": 100,
    "bronze.historical_news_gdelt": 100,
    "silver.stock_prices": 1000,
    "silver.technical_indicators": 500,
    "silver.sector_features": 10,
    "silver.market_breadth": 10,
    "gold.company_universe": 10,
    "gold.portfolio_holdings": 10,
    "gold.portfolio_managers": 1,
    "gold.portfolio_risk_metrics": 1,
    "gold.stress_test_results": 1,
    "gold.stock_forecasts": 10,
    "gold.decision_signals": 10,
    "gold.ml_stock_predictions": 10,
}

for table, min_rows in required_tables.items():
    fqn = f"{catalog}.{table}"
    try:
        cnt = spark.table(fqn).count()
        check(f"row_count({table})", cnt >= min_rows, f"{cnt:,} rows (min: {min_rows})")
    except Exception as e:
        check(f"exists({table})", False, str(e)[:100])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Freshness

# COMMAND ----------

print("\n🕐 Data Freshness")
print("-" * 40)

freshness_checks = {
    "silver.stock_prices": ("date", 5),
    "gold.portfolio_risk_metrics": ("computed_at", 7),
    "gold.ml_stock_predictions": ("pred_date", 7),
    "gold.stock_forecasts": ("forecast_date", 7),
    "gold.decision_signals": ("as_of_date", 7),
    "bronze.news_rss_all": ("published_date", 3),
}

today = datetime.now().date()
for table, (date_col, max_days) in freshness_checks.items():
    fqn = f"{catalog}.{table}"
    try:
        row = spark.sql(f"SELECT MAX({date_col}) AS latest FROM {fqn}").first()
        latest = row["latest"]
        if latest is None:
            check(f"freshness({table})", False, f"{date_col} is all NULL")
        else:
            latest_date = latest.date() if hasattr(latest, 'date') else latest
            age_days = (today - latest_date).days
            check(f"freshness({table})", age_days <= max_days, f"latest={latest_date}, age={age_days}d (max: {max_days}d)")
    except Exception as e:
        check(f"freshness({table})", False, str(e)[:100])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Null Rate Checks

# COMMAND ----------

print("\n🔍 Critical Column Null Checks")
print("-" * 40)

null_checks = [
    ("gold.company_universe", ["symbol", "company_name", "sector"]),
    ("gold.portfolio_holdings", ["symbol", "manager_id", "weight"]),
    ("gold.portfolio_managers", ["manager_name", "risk_profile", "aum_usd"]),
    ("silver.stock_prices", ["symbol", "close", "date"]),
    ("gold.ml_stock_predictions", ["symbol", "direction", "confidence"]),
]

for table, columns in null_checks:
    fqn = f"{catalog}.{table}"
    for col in columns:
        try:
            row = spark.sql(f"SELECT COUNT(*) AS total, SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls FROM {fqn}").first()
            null_pct = (row["nulls"] / max(row["total"], 1)) * 100
            check(f"null_rate({table}.{col})", null_pct == 0, f"{row['nulls']}/{row['total']} nulls ({null_pct:.1f}%)")
        except Exception as e:
            check(f"null_rate({table}.{col})", False, str(e)[:100])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cross-Table Consistency

# COMMAND ----------

print("\n🔗 Cross-Table Consistency")
print("-" * 40)

# All portfolio holdings symbols should be in company_universe
try:
    orphans = spark.sql(f"""
        SELECT DISTINCT h.symbol FROM {catalog}.gold.portfolio_holdings h
        LEFT JOIN {catalog}.gold.company_universe u ON h.symbol = u.symbol
        WHERE u.symbol IS NULL
    """).count()
    check("holdings_in_universe", orphans == 0, f"{orphans} orphan symbols in holdings")
except Exception as e:
    check("holdings_in_universe", False, str(e)[:100])

# All managers in risk_metrics should be in portfolio_managers
try:
    orphans = spark.sql(f"""
        SELECT DISTINCT r.manager_name FROM {catalog}.gold.portfolio_risk_metrics r
        LEFT JOIN {catalog}.gold.portfolio_managers m ON r.manager_name = m.manager_name
        WHERE m.manager_name IS NULL
    """).count()
    check("risk_metrics_managers_valid", orphans == 0, f"{orphans} orphan managers in risk_metrics")
except Exception as e:
    check("risk_metrics_managers_valid", False, str(e)[:100])

# ML predictions symbols should be in company_universe
try:
    orphans = spark.sql(f"""
        SELECT DISTINCT p.symbol FROM {catalog}.gold.ml_stock_predictions p
        LEFT JOIN {catalog}.gold.company_universe u ON p.symbol = u.symbol
        WHERE u.symbol IS NULL
    """).count()
    check("ml_preds_in_universe", orphans == 0, f"{orphans} orphan symbols in ml_predictions")
except Exception as e:
    check("ml_preds_in_universe", False, str(e)[:100])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

passed = sum(1 for r in results if r["status"] == "PASS")
failed = sum(1 for r in results if r["status"] == "FAIL")
total = len(results)

print(f"\n{'='*60}")
print(f"DATA QUALITY SUMMARY")
print(f"{'='*60}")
print(f"  Total checks: {total}")
print(f"  ✅ Passed: {passed}")
print(f"  ❌ Failed: {failed}")
print(f"  Score: {passed/max(total,1)*100:.0f}%")
print(f"{'='*60}")

if failed > 0:
    print("\n❌ FAILED CHECKS:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  - {r['check']}: {r['detail']}")

# Write results to Delta for monitoring
from pyspark.sql import Row
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.monitoring")

rows = [Row(
    check_time=datetime.now(timezone.utc).isoformat(),
    check_name=r["check"],
    status=r["status"],
    detail=r["detail"]
) for r in results]

spark.createDataFrame(rows).write.mode("append").saveAsTable(f"{catalog}.monitoring.data_quality_log")
print(f"\n📝 Results logged to {catalog}.monitoring.data_quality_log")

# Fail the job if critical checks fail
if failed > 0:
    alert_mode = dbutils.widgets.get("alert_on_failure").strip().lower()
    if alert_mode == "true":
        raise Exception(f"DATA QUALITY: {failed}/{total} checks failed. See details above.")
