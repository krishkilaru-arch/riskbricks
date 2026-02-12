# Databricks notebook source
# MAGIC %md
# MAGIC # ⏱️ Hourly Intraday Stock Prices → Gold
# MAGIC
# MAGIC **Purpose**: Fetch hourly intraday prices from Yahoo Finance and upsert into Gold.
# MAGIC
# MAGIC **Target**: `riskbricks.gold.stock_prices_intraday`

# COMMAND ----------

dbutils.widgets.text("lookback_days", "1", "Lookback days (yfinance limit)")
dbutils.widgets.dropdown("latest_only", "true", ["true", "false"], "Keep only latest bar per symbol")

# COMMAND ----------

%pip install yfinance pandas
dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, DoubleType, LongType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

lookback_days = int(dbutils.widgets.get("lookback_days") or "1")
latest_only = dbutils.widgets.get("latest_only").strip().lower() == "true"

symbols_df = spark.sql("""
    SELECT DISTINCT symbol
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")
symbols = [row.symbol for row in symbols_df.collect()]
if not symbols:
    raise ValueError("No symbols found in gold.company_universe.")

print(f"✅ Symbols loaded from company_universe: {len(symbols)}")
print(f"✅ Lookback days: {lookback_days}")
print(f"✅ Latest only: {latest_only}")

# COMMAND ----------

def fetch_intraday(symbols, lookback_days=7):
    tickers = symbols if len(symbols) > 1 else symbols[0]
    data = yf.download(
        tickers=tickers,
        period=f"{lookback_days}d",
        interval="60m",
        group_by="ticker" if isinstance(tickers, list) else None,
        auto_adjust=False,
        progress=False,
        actions=True,
    )
    if data is None or data.empty:
        return pd.DataFrame()
    return data

# COMMAND ----------

data = fetch_intraday(symbols, lookback_days=lookback_days)
if data.empty:
    dbutils.notebook.exit("No intraday data returned.")

print(f"✅ Intraday rows fetched: {len(data)}")

# COMMAND ----------

def to_long(df, symbols):
    if isinstance(df.columns, pd.MultiIndex):
        symbol_level = 1
        if "Ticker" in df.columns.names:
            symbol_level = "Ticker"
        elif any(s in df.columns.get_level_values(0) for s in symbols[:3]):
            symbol_level = 0
        long_df = df.stack(level=symbol_level, future_stack=True).reset_index()
        if hasattr(long_df.columns, "name"):
            long_df.columns.name = None
        rename_map = {
            "level_1": "symbol",
            "Ticker": "symbol",
            "Datetime": "event_ts",
            "Date": "event_ts",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits",
            "Capital Gains": "capital_gains",
        }
        long_df = long_df.rename(columns=rename_map)
    else:
        long_df = df.reset_index()
        long_df = long_df.rename(columns={
            "Datetime": "event_ts",
            "Date": "event_ts",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits",
            "Capital Gains": "capital_gains",
        })
        long_df["symbol"] = symbols[0]

    # Ensure we have an event timestamp column
    if "event_ts" not in long_df.columns:
        # Try common reset_index column names
        for col in ("index", "level_0", "Datetime", "Date"):
            if col in long_df.columns:
                long_df = long_df.rename(columns={col: "event_ts"})
                break
    if "event_ts" not in long_df.columns:
        raise KeyError(f"event_ts not found. Columns: {list(long_df.columns)}")

    if "adj_close" not in long_df.columns and "close" in long_df.columns:
        long_df["adj_close"] = long_df["close"]
    for col in ("dividends", "stock_splits", "capital_gains"):
        if col not in long_df.columns:
            long_df[col] = 0.0
    if "volume" in long_df.columns:
        long_df["volume"] = long_df["volume"].fillna(0).astype("int64")

    long_df["event_ts"] = pd.to_datetime(long_df["event_ts"])
    long_df["event_date"] = long_df["event_ts"].dt.date
    long_df = long_df[long_df["close"].notna()]
    return long_df

long_df = to_long(data, symbols)
if latest_only:
    long_df = long_df.sort_values(["symbol", "event_ts"]).groupby("symbol").tail(1)
    print(f"✅ Latest-only rows: {len(long_df)}")
print(f"✅ Intraday long rows: {len(long_df)}")

# COMMAND ----------

schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("event_date", DateType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("dividends", DoubleType(), True),
    StructField("stock_splits", DoubleType(), True),
    StructField("capital_gains", DoubleType(), True),
])

spark_df = spark.createDataFrame(long_df.to_dict("records"), schema=schema)
spark_df = spark_df.withColumn("source", F.lit("yfinance_intraday")) \
    .withColumn("ingestion_timestamp", F.current_timestamp())

target_tbl = f"{gold_db}.stock_prices_intraday"

if not spark.catalog.tableExists(target_tbl):
    spark_df.write \
        .mode("overwrite") \
        .partitionBy("event_date", "symbol") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_tbl)
else:
    spark_df.createOrReplaceTempView("intraday_updates")
    spark.sql(f"""
        MERGE INTO {target_tbl} t
        USING intraday_updates s
        ON t.symbol = s.symbol AND t.event_ts = s.event_ts
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

total = spark.sql(f"SELECT COUNT(*) AS c FROM {target_tbl}").collect()[0]["c"]
print(f"✅ Gold stock_prices_intraday: {total} rows")
