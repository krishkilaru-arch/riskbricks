# Databricks notebook source
# MAGIC %md
# MAGIC # 📉 Risk Agent (Gold-Based)
# MAGIC
# MAGIC Computes daily risk metrics using gold price data.

# COMMAND ----------

dbutils.widgets.text("symbol", "NVDA", "Symbol (or ALL)")
dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_days", "252", "Lookback days")
dbutils.widgets.text("order_size", "1000000", "Order size (USD)")
dbutils.widgets.text("impact_factor", "0.1", "Impact factor")
dbutils.widgets.text("max_symbols", "0", "Max symbols when ALL (0 = all)")

# COMMAND ----------

# MAGIC %pip install pandas numpy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, TimestampType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

symbol = dbutils.widgets.get("symbol").strip().upper()
as_of_date = dbutils.widgets.get("as_of_date").strip()
lookback_days = int(dbutils.widgets.get("lookback_days") or "252")
order_size = float(dbutils.widgets.get("order_size") or "1000000")
impact_factor = float(dbutils.widgets.get("impact_factor") or "0.1")
max_symbols = int(dbutils.widgets.get("max_symbols") or "0")

local_tz = ZoneInfo("America/New_York")
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

end = datetime.strptime(as_of_date, "%Y-%m-%d").date()
start = end - timedelta(days=lookback_days * 2)

print(f"✅ Symbol: {symbol}")
print(f"✅ As of date: {as_of_date}")

if symbol == "ALL":
    symbols_df = spark.sql("""
        SELECT DISTINCT symbol
        FROM riskbricks.gold.company_universe
        ORDER BY symbol
    """)
    symbols = [row.symbol for row in symbols_df.collect()]
    if max_symbols and max_symbols > 0:
        symbols = symbols[:max_symbols]
    for sym in symbols:
        dbutils.notebook.run(
            "/Workspace/Shared/RiskBricks/files/notebooks/agents/03_risk_agent",
            0,
            {
                "symbol": sym,
                "as_of_date": as_of_date,
                "lookback_days": str(lookback_days),
                "order_size": str(order_size),
                "impact_factor": str(impact_factor),
            }
        )
    dbutils.notebook.exit("✅ Risk agent complete for ALL symbols")

prices_tbl = f"{gold_db}.stock_prices_daily"
if not spark.catalog.tableExists(prices_tbl):
    raise ValueError(f"Missing table: {prices_tbl}")

prices_df = spark.table(prices_tbl).filter(
    (F.col("date") >= F.lit(start).cast("date")) &
    (F.col("date") <= F.lit(end).cast("date"))
).select(
    "symbol",
    F.col("date").alias("date"),
    F.coalesce(F.col("adj_close"), F.col("close")).alias("close"),
    F.col("volume").alias("volume"),
)

if prices_df.limit(1).count() == 0:
    raise ValueError(f"No price data available in {prices_tbl} for window.")

prices_pd = prices_df.toPandas()
prices_pd["date"] = pd.to_datetime(prices_pd["date"])
prices_pd = prices_pd.dropna(subset=["close"])

symbol_pd = prices_pd[prices_pd["symbol"] == symbol].sort_values("date")
if symbol_pd.empty or len(symbol_pd) < 20:
    raise ValueError(f"Insufficient price history for {symbol}")

symbol_pd = symbol_pd.set_index("date")
symbol_ret = np.log(symbol_pd["close"] / symbol_pd["close"].shift(1)).dropna()
symbol_ret = symbol_ret.tail(lookback_days)

market_pd = prices_pd.copy()
market_pd = market_pd.sort_values(["symbol", "date"])
market_pd["ret"] = market_pd.groupby("symbol")["close"].transform(lambda s: np.log(s / s.shift(1)))
market_ret = market_pd.dropna(subset=["ret"]).groupby("date")["ret"].mean().sort_index()
market_ret = market_ret.tail(lookback_days)

common_idx = symbol_ret.index.intersection(market_ret.index)
symbol_ret = symbol_ret.loc[common_idx]
market_ret = market_ret.loc[common_idx]

def _ewma_vol(returns, lam=0.94):
    var = 0.0
    for r in returns:
        var = lam * var + (1 - lam) * (r ** 2)
    return float(np.sqrt(var))

ewma_vol = _ewma_vol(symbol_ret.values)
vol_20d = float(symbol_ret.tail(20).std(ddof=1)) if len(symbol_ret) >= 20 else None
vol_60d = float(symbol_ret.tail(60).std(ddof=1)) if len(symbol_ret) >= 60 else None
vol_252d = float(symbol_ret.std(ddof=1)) if len(symbol_ret) > 1 else None

beta = None
if len(common_idx) > 5 and market_ret.var(ddof=1) > 0:
    beta = float(np.cov(symbol_ret, market_ret, ddof=1)[0, 1] / market_ret.var(ddof=1))

prices_series = symbol_pd["close"].tail(lookback_days)
last_price = float(prices_series.iloc[-1])
rolling_max = prices_series.cummax()
drawdown = prices_series / rolling_max - 1.0
max_drawdown = float(drawdown.min())

var_95 = float(np.percentile(symbol_ret, 5))
es_95 = float(symbol_ret[symbol_ret <= var_95].mean()) if len(symbol_ret) else None

adv = float(symbol_pd["volume"].tail(20).mean()) if "volume" in symbol_pd else None
impact = None
if adv and adv > 0 and last_price:
    participation = order_size / (adv * last_price)
    impact = float(participation * impact_factor)

risk_tbl = f"{gold_db}.risk_metrics_daily"
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("as_of_date", DateType(), False),
    StructField("lookback_days", IntegerType(), False),
    StructField("ewma_vol", DoubleType(), True),
    StructField("vol_20d", DoubleType(), True),
    StructField("vol_60d", DoubleType(), True),
    StructField("vol_252d", DoubleType(), True),
    StructField("beta_1y", DoubleType(), True),
    StructField("max_drawdown", DoubleType(), True),
    StructField("var_95", DoubleType(), True),
    StructField("es_95", DoubleType(), True),
    StructField("adv_20d", DoubleType(), True),
    StructField("impact", DoubleType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
])

row = (
    symbol,
    end,
    int(lookback_days),
    ewma_vol,
    vol_20d,
    vol_60d,
    vol_252d,
    beta,
    max_drawdown,
    var_95,
    es_95,
    adv,
    impact,
    datetime.now(local_tz),
)

df = spark.createDataFrame([row], schema=schema)
if not spark.catalog.tableExists(risk_tbl):
    df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(risk_tbl)
else:
    df.createOrReplaceTempView("risk_updates")
    spark.sql(f"""
        MERGE INTO {risk_tbl} t
        USING risk_updates s
        ON t.symbol = s.symbol AND t.as_of_date = s.as_of_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# Validation summary (counts after write)
row_count = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM {risk_tbl}
    WHERE symbol = '{symbol}' AND as_of_date = DATE('{as_of_date}')
""").collect()[0]["c"]
print(f"📊 Validation: risk_metrics_daily rows for {symbol} on {as_of_date}: {row_count}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"symbol": symbol, "as_of_date": as_of_date}))
