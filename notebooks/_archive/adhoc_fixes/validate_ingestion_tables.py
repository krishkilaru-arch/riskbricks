# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ Ingestion Data Validation
# MAGIC
# MAGIC Validates core ingestion tables (Bronze/Silver/Gold) with row counts,
# MAGIC date coverage, and null-rate checks.

# COMMAND ----------

dbutils.widgets.text("days_back", "30", "Days back for coverage checks")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
silver_db = f"{catalog}.silver"
bronze_db = f"{catalog}.bronze"

days_back = int(dbutils.widgets.get("days_back") or "30")
local_tz = ZoneInfo("America/New_York")
as_of_date = datetime.now(local_tz).date()
start_date = as_of_date - timedelta(days=days_back)

print(f"✅ Validation window: {start_date} → {as_of_date}")

# COMMAND ----------

def table_exists(table_name):
    return spark.catalog.tableExists(table_name)

def coverage_check(table_name, date_col, group_col=None):
    if not table_exists(table_name):
        print(f"⚠️ Missing table: {table_name}")
        return
    df = spark.table(table_name).filter(
        (F.col(date_col) >= F.lit(start_date).cast("date")) &
        (F.col(date_col) <= F.lit(as_of_date).cast("date"))
    )
    if group_col:
        df.groupBy(group_col).agg(
            F.count("*").alias("rows"),
            F.min(date_col).alias("min_date"),
            F.max(date_col).alias("max_date")
        ).orderBy(group_col).show(50, truncate=False)
    else:
        df.agg(
            F.count("*").alias("rows"),
            F.min(date_col).alias("min_date"),
            F.max(date_col).alias("max_date")
        ).show(truncate=False)

def null_rate_check(table_name, cols):
    if not table_exists(table_name):
        return
    df = spark.table(table_name)
    exprs = []
    for c in cols:
        exprs.append((F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*")).alias(f"null_rate_{c}"))
    df.select(exprs).show(truncate=False)

# COMMAND ----------

print("## Bronze")
coverage_check(f"{bronze_db}.stock_prices_bronze", "date", "symbol")
coverage_check(f"{bronze_db}.macro_indicators_bronze", "date", "indicator_name")
coverage_check(f"{bronze_db}.historical_news_gdelt", "event_date", "symbol")
coverage_check(f"{bronze_db}.news_rss_all", "published_date", "source")

print("## Silver")
coverage_check(f"{silver_db}.news_rss_combined", "published_date", "symbol")

print("## Gold")
coverage_check(f"{gold_db}.stock_prices_daily", "date", "symbol")
coverage_check(f"{gold_db}.stock_prices_intraday", "event_date", "symbol")
coverage_check(f"{gold_db}.macro_indicators_daily", "date", "indicator_name")
coverage_check(f"{gold_db}.rag_corpus", "published_date", "symbol")
coverage_check(f"{gold_db}.earnings_calendar", "event_date", "symbol")
coverage_check(f"{gold_db}.analyst_recommendations", "event_date", "symbol")
coverage_check(f"{gold_db}.options_iv_skew_daily", "as_of_date", "symbol")
coverage_check(f"{gold_db}.short_interest_snapshot", "as_of_date", "symbol")
coverage_check(f"{gold_db}.sec_fundamentals", "as_of_date", "symbol")
coverage_check(f"{gold_db}.insider_form4", "filing_date", "symbol")

print("## Null-rate checks (Gold)")
null_rate_check(f"{gold_db}.stock_prices_daily", ["close", "volume", "adj_close"])
null_rate_check(f"{gold_db}.macro_indicators_daily", ["value"])
null_rate_check(f"{gold_db}.earnings_calendar", ["eps_actual", "eps_estimate"])
null_rate_check(f"{gold_db}.options_iv_skew_daily", ["call_iv", "put_iv", "iv_skew"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

