# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 Alternative Signals (Yahoo Finance)
# MAGIC
# MAGIC **Purpose**: Ingest free signals for portfolio managers:
# MAGIC - Earnings calendar + EPS surprises
# MAGIC - Analyst recommendations / ratings changes
# MAGIC - Options implied volatility + skew
# MAGIC - Short interest snapshot
# MAGIC
# MAGIC **Scope**: Top 20 most liquid US stocks (AAPL, MSFT, GOOGL, AMZN, NVDA, etc.)

# COMMAND ----------

dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")
dbutils.widgets.text("max_symbols", "0", "Max symbols (0 = all 20)")
dbutils.widgets.text("sleep_seconds", "0.2", "Sleep between symbols")
dbutils.widgets.text("earnings_limit", "60", "Max earnings rows per symbol")

# COMMAND ----------

%pip install yfinance pandas numpy
dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
import pandas as pd
import yfinance as yf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

local_tz = ZoneInfo("America/New_York")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

max_symbols = int(dbutils.widgets.get("max_symbols") or "0")
sleep_seconds = float(dbutils.widgets.get("sleep_seconds") or "0.2")
earnings_limit = int(dbutils.widgets.get("earnings_limit") or "60")

# Top 20 most liquid, famous stocks (curated list - no data quality issues)
TOP_20_STOCKS = [
    'AAPL',   # Apple
    'MSFT',   # Microsoft
    'GOOGL',  # Alphabet (Google)
    'AMZN',   # Amazon
    'NVDA',   # Nvidia
    'META',   # Meta (Facebook)
    'TSLA',   # Tesla
    'JPM',    # JPMorgan Chase
    'V',      # Visa
    'WMT',    # Walmart
    'JNJ',    # Johnson & Johnson
    'PG',     # Procter & Gamble
    'MA',     # Mastercard
    'HD',     # Home Depot
    'BAC',    # Bank of America
    'XOM',    # Exxon Mobil
    'CVX',    # Chevron
    'KO',     # Coca-Cola
    'DIS',    # Disney
    'NFLX',   # Netflix
]

# Use top 20 stocks instead of full company_universe
from pyspark.sql.types import StructType, StructField, StringType as ST
symbols_schema = StructType([StructField("symbol", ST(), False)])
symbols_df = spark.createDataFrame([(s,) for s in TOP_20_STOCKS], schema=symbols_schema)
symbols = [row.symbol for row in symbols_df.collect()]
if max_symbols and max_symbols > 0:
    symbols = symbols[:max_symbols]

print(f"🎯 Top 20 US Stocks Mode")
print(f"✅ As of date: {as_of_date}")
print(f"✅ Symbols to process: {len(symbols)}")
print(f"✅ Earnings limit per symbol: {earnings_limit}")
print()
print("📋 Symbols:", ", ".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""))
print()

# COMMAND ----------

def safe_info(ticker):
    try:
        return ticker.info or {}
    except Exception:
        return {}

def _to_float(val):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

def _safe_date(val):
    """Convert pandas date/timestamp to datetime, handling NaT"""
    if pd.isna(val):
        return None
    try:
        # Convert pandas Timestamp to Python datetime
        if hasattr(val, 'to_pydatetime'):
            return val.to_pydatetime()
        elif hasattr(val, 'date'):
            return datetime.combine(val.date(), datetime.min.time())
        else:
            return pd.to_datetime(val).to_pydatetime() if pd.notna(val) else None
    except Exception:
        return None

def _safe_numeric(val):
    """Convert to float, handling NaN/NaT"""
    if pd.isna(val):
        return None
    try:
        return float(val)
    except Exception:
        return None

earnings_rows = []
analyst_rows = []
options_rows = []
short_rows = []

successful = 0
failed = 0

for i, sym in enumerate(symbols):
    try:
        t = yf.Ticker(sym)
        
        # Quick validation - check if ticker has basic info
        try:
            info = safe_info(t)
            if not info or len(info) < 5:
                failed += 1
                continue
        except Exception:
            failed += 1
            continue

        # Earnings calendar / EPS surprises
        try:
            earnings = t.get_earnings_dates(limit=earnings_limit)
            if earnings is not None and not earnings.empty:
                earnings = earnings.reset_index()
                for _, row in earnings.iterrows():
                    earnings_rows.append({
                        "symbol": sym,
                        "event_date": _safe_date(row.get("Earnings Date")),
                        "eps_estimate": _safe_numeric(row.get("EPS Estimate")),
                        "eps_actual": _safe_numeric(row.get("Reported EPS")),
                        "surprise_pct": _safe_numeric(row.get("Surprise(%)")),
                        "as_of_date": as_of_date,
                    })
            else:
                # Fallback: try next earnings date from calendar if available
                cal = getattr(t, "calendar", None)
                if cal is not None and not cal.empty and "Earnings Date" in cal.index:
                    cal_row = cal.loc["Earnings Date"]
                    next_date = None
                    if hasattr(cal_row, "values"):
                        next_date = cal_row.values[0]
                    else:
                        next_date = cal_row
                    if isinstance(next_date, (list, tuple)) and next_date:
                        next_date = next_date[0]
                    if next_date:
                        eps_est = None
                        if "Earnings Average" in cal.index:
                            try:
                                eps_est = float(cal.loc["Earnings Average"].values[0])
                            except Exception:
                                eps_est = None
                        earnings_rows.append({
                            "symbol": sym,
                            "event_date": _safe_date(next_date),
                            "eps_estimate": _safe_numeric(eps_est),
                            "eps_actual": None,
                            "surprise_pct": None,
                            "as_of_date": as_of_date,
                        })
        except Exception:
            pass

        # Analyst recommendations / ratings changes
        try:
            recs = t.recommendations
            if recs is not None and not recs.empty:
                recs = recs.reset_index()
                for _, row in recs.tail(50).iterrows():
                    analyst_rows.append({
                        "symbol": sym,
                        "event_date": _safe_date(row.get("Date")),
                        "firm": row.get("Firm"),
                        "to_grade": row.get("To Grade"),
                        "from_grade": row.get("From Grade"),
                        "action": row.get("Action"),
                        "as_of_date": as_of_date,
                    })
        except Exception:
            pass

        # Options IV + skew
        try:
            expirations = t.options or []
            if expirations:
                exp = expirations[0]
                chain = t.option_chain(exp)
                calls = chain.calls
                puts = chain.puts
                call_iv = float(calls["impliedVolatility"].median()) if not calls.empty else None
                put_iv = float(puts["impliedVolatility"].median()) if not puts.empty else None
                options_rows.append({
                    "symbol": sym,
                    "expiration": exp,
                    "call_iv": call_iv,
                    "put_iv": put_iv,
                    "iv_skew": (put_iv - call_iv) if call_iv is not None and put_iv is not None else None,
                    "as_of_date": as_of_date,
                })
        except Exception:
            pass

        # Short interest snapshot (best-effort from Yahoo info)
        info = safe_info(t)
        short_rows.append({
            "symbol": sym,
            "short_ratio": _to_float(info.get("shortRatio")),
            "short_percent_float": _to_float(info.get("shortPercentOfFloat")),
            "shares_short": _to_float(info.get("sharesShort")),
            "shares_short_prior": _to_float(info.get("sharesShortPriorMonth")),
            "short_interest": _to_float(info.get("shortInterest")),
            "borrow_rate": None,
            "as_of_date": as_of_date,
        })

        successful += 1
        
        # Progress indicator
        if (i + 1) % 10 == 0:
            print(f"Progress: {i+1}/{len(symbols)} symbols processed (✅ {successful}, ❌ {failed})")
        
        time.sleep(sleep_seconds)
    except Exception as e:
        failed += 1
        continue

print()
print("="*60)
print(f"✅ Successfully processed: {successful}/{len(symbols)} symbols")
print(f"❌ Failed/skipped: {failed}/{len(symbols)} symbols")
print("="*60)

# COMMAND ----------

def write_table(table_name, rows, schema):
    if not rows:
        if spark.catalog.tableExists(table_name):
            print(f"⚠️ No rows for {table_name}")
            return
        empty_df = spark.createDataFrame([], schema=schema)
        empty_df = empty_df.withColumn("ingestion_timestamp", F.lit(None).cast("timestamp"))
        empty_df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(table_name)
        print(f"✅ Created empty table {table_name}")
        return
    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    if not spark.catalog.tableExists(table_name):
        df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(table_name)
    else:
        df.write.mode("append").saveAsTable(table_name)
    print(f"✅ Saved {len(rows)} rows to {table_name}")

def log_table_count(table_name, as_of_date_value):
    if not spark.catalog.tableExists(table_name):
        print(f"⚠️ Missing table: {table_name}")
        return
    count = spark.sql(
        f"SELECT COUNT(*) AS c FROM {table_name} WHERE as_of_date = '{as_of_date_value}'"
    ).collect()[0]["c"]
    print(f"📊 {table_name} rows for as_of_date={as_of_date_value}: {count}")

earn_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("event_date", TimestampType(), True),
    StructField("eps_estimate", DoubleType(), True),
    StructField("eps_actual", DoubleType(), True),
    StructField("surprise_pct", DoubleType(), True),
    StructField("as_of_date", StringType(), False),
])

analyst_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("event_date", TimestampType(), True),
    StructField("firm", StringType(), True),
    StructField("to_grade", StringType(), True),
    StructField("from_grade", StringType(), True),
    StructField("action", StringType(), True),
    StructField("as_of_date", StringType(), False),
])

options_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("expiration", StringType(), True),
    StructField("call_iv", DoubleType(), True),
    StructField("put_iv", DoubleType(), True),
    StructField("iv_skew", DoubleType(), True),
    StructField("as_of_date", StringType(), False),
])

short_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("short_ratio", DoubleType(), True),
    StructField("short_percent_float", DoubleType(), True),
    StructField("shares_short", DoubleType(), True),
    StructField("shares_short_prior", DoubleType(), True),
    StructField("short_interest", DoubleType(), True),
    StructField("borrow_rate", DoubleType(), True),
    StructField("as_of_date", StringType(), False),
])

write_table(f"{gold_db}.earnings_calendar", earnings_rows, earn_schema)
write_table(f"{gold_db}.analyst_recommendations", analyst_rows, analyst_schema)
write_table(f"{gold_db}.options_iv_skew_daily", options_rows, options_schema)
write_table(f"{gold_db}.short_interest_snapshot", short_rows, short_schema)

log_table_count(f"{gold_db}.earnings_calendar", as_of_date)
log_table_count(f"{gold_db}.analyst_recommendations", as_of_date)
log_table_count(f"{gold_db}.options_iv_skew_daily", as_of_date)
log_table_count(f"{gold_db}.short_interest_snapshot", as_of_date)

dbutils.notebook.exit("✅ Alternative signals ingestion complete")
