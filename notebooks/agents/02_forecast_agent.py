# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Forecast Agent (Multi-Model)
# MAGIC
# MAGIC Produces forecasts using price history plus news and alternative signals.

# COMMAND ----------

dbutils.widgets.text("symbol", "NVDA", "Symbol (or ALL)")
dbutils.widgets.text("target_date", "", "Target date (YYYY-MM-DD)")
dbutils.widgets.dropdown("method", "mean", ["mean", "gbm"], "Forecast method")
dbutils.widgets.dropdown("mode", "full", ["full", "fast", "custom"], "Run mode")
dbutils.widgets.text("models", "mean,gbm,arima,xgboost,lstm,ridge,news_event", "Models to run")
dbutils.widgets.text("train_days", "252", "Training window (days)")
dbutils.widgets.text("lstm_epochs", "5", "LSTM epochs")
dbutils.widgets.text("xgb_estimators", "200", "XGBoost estimators")
dbutils.widgets.text("max_symbols", "0", "Max symbols when ALL (0 = all)")

# COMMAND ----------

# MAGIC %pip install pandas numpy scikit-learn statsmodels xgboost tensorflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import sys
import time
import uuid
import pandas as pd
import numpy as np

try:
    from statsmodels.tsa.arima.model import ARIMA
except Exception:
    ARIMA = None

try:
    from xgboost import XGBRegressor
except Exception:
    XGBRegressor = None

try:
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
except Exception:
    Ridge = None
    StandardScaler = None
    Pipeline = None

try:
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense
except Exception:
    Sequential = None
    LSTM = None
    Dense = None

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType, IntegerType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

symbol = dbutils.widgets.get("symbol").strip().upper()
target_date = dbutils.widgets.get("target_date").strip()
method = dbutils.widgets.get("method").strip()
mode = dbutils.widgets.get("mode").strip().lower()
models = [m.strip().lower() for m in dbutils.widgets.get("models").split(",") if m.strip()]
train_days = int(dbutils.widgets.get("train_days") or "252")
lstm_epochs = int(dbutils.widgets.get("lstm_epochs") or "5")
xgb_estimators = int(dbutils.widgets.get("xgb_estimators") or "200")
max_symbols = int(dbutils.widgets.get("max_symbols") or "0")

local_tz = ZoneInfo("America/New_York")
if mode == "fast":
    models = ["mean", "gbm", "ridge", "news_event"]
    train_days = 120
    lstm_epochs = 2
    xgb_estimators = 50
elif mode == "full":
    pass

if not target_date:
    target_date = (datetime.now(local_tz).date() + timedelta(days=1)).strftime("%Y-%m-%d")

run_id = str(uuid.uuid4())
run_start = time.time()

print(f"✅ Symbol: {symbol}")
print(f"✅ Target date: {target_date}")
print(f"✅ Method: {method}")
print(f"✅ Mode: {mode}")
print(f"✅ Models: {models}")

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
            "/Workspace/Shared/RiskBricks/files/notebooks/agents/02_forecast_agent",
            0,
            {
                "symbol": sym,
                "target_date": target_date,
                "method": method,
                "mode": mode,
                "models": ",".join(models),
                "train_days": str(train_days),
                "lstm_epochs": str(lstm_epochs),
                "xgb_estimators": str(xgb_estimators),
            }
        )
    dbutils.notebook.exit("✅ Forecast agent complete for ALL symbols")

missing_notes = []

prices_tbl = f"{gold_db}.stock_prices_daily"
if not spark.catalog.tableExists(prices_tbl):
    raise ValueError(f"Missing table: {prices_tbl}")

prices_df = spark.table(prices_tbl) \
    .filter(F.col("symbol") == F.lit(symbol)) \
    .select(
        F.col("date").alias("date"),
        F.coalesce(F.col("adj_close"), F.col("close")).alias("close")
    ) \
    .orderBy("date")

if prices_df.limit(1).count() == 0:
    raise ValueError(f"No price data found for {symbol} in {prices_tbl}")

prices_pd = prices_df.toPandas()
prices_pd["date"] = pd.to_datetime(prices_pd["date"])
prices_pd = prices_pd.dropna(subset=["close"])
if prices_pd.empty or len(prices_pd) < 2:
    raise ValueError(f"Insufficient price history for {symbol}")

prices_pd = prices_pd.set_index("date")
close = prices_pd["close"]
last_date = close.index[-1].date()
last_price = float(close.iloc[-1])

target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
if target_dt <= last_date:
    # Auto-shift to next business day to avoid same-day leakage.
    next_bd = pd.bdate_range(last_date, periods=2, freq="B")[-1].date()
    print(f"⚠️ Target date {target_dt} <= last available date {last_date}; using next business day {next_bd}.")
    target_dt = next_bd
    target_date = target_dt.strftime("%Y-%m-%d")

bdays = pd.bdate_range(last_date, target_dt)
horizon_days = max(len(bdays) - 1, 0)
rets = np.log(close / close.shift(1)).dropna()
mu = float(rets.mean())
sigma = float(rets.std(ddof=1))

# Build feature frame
features = pd.DataFrame(index=close.index)
features["return_1d"] = np.log(close / close.shift(1))
features["return_5d"] = np.log(close / close.shift(5))
features["vol_20d"] = features["return_1d"].rolling(20).std(ddof=1)
features["ma_20"] = close.rolling(20).mean()
features["ma_50"] = close.rolling(50).mean()
features["momentum_20"] = close / features["ma_20"] - 1.0
features["momentum_50"] = close / features["ma_50"] - 1.0

# Macro indicators (pivot to columns)
macro_tbl = f"{gold_db}.macro_indicators_daily"
if spark.catalog.tableExists(macro_tbl):
    macro_df = spark.table(macro_tbl).select("date", "indicator_name", "value")
    macro_pd = macro_df.toPandas()
    if not macro_pd.empty:
        macro_pd["date"] = pd.to_datetime(macro_pd["date"])
        macro_pivot = macro_pd.pivot_table(index="date", columns="indicator_name", values="value", aggfunc="last")
        macro_pivot = macro_pivot.sort_index().ffill()
        features = features.join(macro_pivot, how="left")
    else:
        missing_notes.append("macro_indicators_daily has no rows")
else:
    missing_notes.append("macro_indicators_daily table missing")

# RAG corpus features (doc counts)
rag_tbl = f"{gold_db}.rag_corpus"
if spark.catalog.tableExists(rag_tbl):
    rag_df = spark.table(rag_tbl).filter(F.col("symbol") == F.lit(symbol)) \
        .groupBy("published_date").agg(
            F.count("*").alias("news_doc_count"),
            F.countDistinct("source").alias("news_source_count"),
        )
    rag_pd = rag_df.toPandas()
    if not rag_pd.empty:
        rag_pd["published_date"] = pd.to_datetime(rag_pd["published_date"])
        rag_pd = rag_pd.set_index("published_date")
        features = features.join(rag_pd, how="left")
    else:
        missing_notes.append("rag_corpus has no rows for symbol")
else:
    missing_notes.append("rag_corpus table missing")

# News impact history features (symbol-level historical impact)
news_impact_tbl = f"{gold_db}.news_impact_history"
if spark.catalog.tableExists(news_impact_tbl):
    impact_stats = spark.table(news_impact_tbl).filter(
        F.col("symbol") == F.lit(symbol)
    ).agg(
        F.avg("impact_1d_pct").alias("news_impact_1d_avg"),
        F.avg(F.abs(F.col("impact_1d_pct"))).alias("news_impact_1d_abs_avg"),
        F.count("*").alias("news_impact_event_count"),
    ).collect()[0]
    if impact_stats["news_impact_event_count"] == 0:
        missing_notes.append("news_impact_history has no rows for symbol")
    else:
        features["news_impact_1d_avg"] = float(impact_stats["news_impact_1d_avg"])
        features["news_impact_1d_abs_avg"] = float(impact_stats["news_impact_1d_abs_avg"])
        features["news_impact_event_count"] = float(impact_stats["news_impact_event_count"])
else:
    missing_notes.append("news_impact_history table missing")

# Geopolitical risk features (sector-level)
geo_tbl = f"{gold_db}.geopolitical_risk_events"
sector = None
if spark.catalog.tableExists("riskbricks.gold.company_universe"):
    sector_row = spark.table("riskbricks.gold.company_universe") \
        .filter(F.col("symbol") == F.lit(symbol)) \
        .select("sector") \
        .limit(1) \
        .collect()
    if sector_row:
        sector = sector_row[0]["sector"]

if spark.catalog.tableExists(geo_tbl) and sector:
    # Note: affected_sectors is STRING, check if sector is contained in it
    geo_stats = spark.table(geo_tbl) \
        .filter(F.col("is_active") == F.lit(True)) \
        .filter(F.col("affected_sectors").contains(sector) | F.col("affected_sectors").isNull()) \
        .agg(
            F.sum("severity").alias("geo_severity_sum"),
            F.max("severity").alias("geo_severity_max"),
            F.avg("estimated_market_impact_pct").alias("geo_impact_avg"),
            F.count("*").alias("geo_event_count"),
        ).collect()[0]
    if geo_stats["geo_event_count"] == 0:
        missing_notes.append("geopolitical_risk_events has no active events for sector")
    else:
        features["geo_severity_sum"] = float(geo_stats["geo_severity_sum"])
        features["geo_severity_max"] = float(geo_stats["geo_severity_max"])
        features["geo_impact_avg"] = float(geo_stats["geo_impact_avg"])
        features["geo_event_count"] = float(geo_stats["geo_event_count"])
else:
    if not spark.catalog.tableExists(geo_tbl):
        missing_notes.append("geopolitical_risk_events table missing")
    if not sector:
        missing_notes.append("sector missing for symbol in company_universe")

# Alt signals (daily snapshots / events)
def _join_alt_daily(features_df, table_name, date_col, cols, prefix, note_label):
    if not spark.catalog.tableExists(table_name):
        missing_notes.append(f"{note_label} table missing")
        return features_df
    df = spark.table(table_name).filter(F.col("symbol") == F.lit(symbol))
    df = df.withColumn("alt_date", F.to_date(F.col(date_col)))
    sel = ["alt_date"] + [F.col(c).alias(f"{prefix}{c}") for c in cols]
    pd_df = df.select(*sel).toPandas()
    if pd_df.empty:
        missing_notes.append(f"{note_label} has no rows for symbol")
        return features_df
    pd_df["alt_date"] = pd.to_datetime(pd_df["alt_date"])
    pd_df = pd_df.dropna(subset=["alt_date"]).set_index("alt_date")
    return features_df.join(pd_df, how="left")

def _join_alt_counts(features_df, table_name, date_col, prefix, note_label):
    if not spark.catalog.tableExists(table_name):
        missing_notes.append(f"{note_label} table missing")
        return features_df
    df = spark.table(table_name).filter(F.col("symbol") == F.lit(symbol))
    df = df.withColumn("alt_date", F.to_date(F.col(date_col)))
    cnt = df.groupBy("alt_date").agg(F.count("*").alias(f"{prefix}count"))
    pd_df = cnt.toPandas()
    if pd_df.empty:
        missing_notes.append(f"{note_label} has no rows for symbol")
        return features_df
    pd_df["alt_date"] = pd.to_datetime(pd_df["alt_date"])
    pd_df = pd_df.set_index("alt_date")
    return features_df.join(pd_df, how="left")

features = _join_alt_counts(features, f"{gold_db}.earnings_calendar", "event_date", "earnings_", "earnings_calendar")
features = _join_alt_counts(features, f"{gold_db}.analyst_recommendations", "event_date", "analyst_", "analyst_recommendations")
features = _join_alt_daily(
    features,
    f"{gold_db}.options_iv_skew_daily",
    "as_of_date",
    ["call_iv", "put_iv", "iv_skew"],
    "options_",
    "options_iv_skew_daily",
)
features = _join_alt_daily(
    features,
    f"{gold_db}.short_interest_snapshot",
    "as_of_date",
    ["short_ratio", "short_percent_float", "shares_short", "short_interest"],
    "short_",
    "short_interest_snapshot",
)
features = _join_alt_daily(
    features,
    f"{gold_db}.sec_fundamentals",
    "as_of_date",
    ["revenue", "eps_basic", "eps_diluted"],
    "sec_",
    "sec_fundamentals",
)

features = features.fillna(0.0)
target_ret = np.log(close.shift(-1) / close)

dataset = features.copy()
dataset["target_ret"] = target_ret
dataset = dataset.dropna()
if dataset.empty:
    raise ValueError("Not enough data to build training set.")

dataset = dataset.tail(train_days)
X = dataset.drop(columns=["target_ret"])
y = dataset["target_ret"].values

def _price_from_daily_ret(pred_ret):
    if horizon_days == 0:
        return last_price, last_price, last_price, last_price, last_price
    expected_price = last_price * float(np.exp(pred_ret * horizon_days))
    vol_term = sigma * np.sqrt(horizon_days)
    lower_1s = last_price * float(np.exp(pred_ret * horizon_days - vol_term))
    upper_1s = last_price * float(np.exp(pred_ret * horizon_days + vol_term))
    lower_2s = last_price * float(np.exp(pred_ret * horizon_days - 2 * vol_term))
    upper_2s = last_price * float(np.exp(pred_ret * horizon_days + 2 * vol_term))
    return expected_price, lower_1s, upper_1s, lower_2s, upper_2s

def _baseline_predict(m):
    if horizon_days == 0:
        pred_ret = 0.0
    elif m == "mean":
        pred_ret = mu
    else:
        pred_ret = mu + 0.5 * sigma**2
    return pred_ret, sigma

def _ridge_predict(x_row, feature_cols):
    if Ridge is None or Pipeline is None:
        return None
    X_train = X[feature_cols].values
    model = Pipeline([("scaler", StandardScaler()), ("ridge", Ridge(alpha=1.0))])
    model.fit(X_train, y)
    pred = float(model.predict(x_row[feature_cols].values.reshape(1, -1))[0])
    resid = y - model.predict(X_train)
    resid_std = float(np.std(resid, ddof=1)) if len(resid) > 1 else sigma
    return pred, resid_std

def _xgb_predict(x_row, feature_cols):
    if XGBRegressor is None:
        return None
    model = XGBRegressor(
        n_estimators=xgb_estimators,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="reg:squarederror",
    )
    model.fit(X[feature_cols].values, y)
    pred = float(model.predict(x_row[feature_cols].values.reshape(1, -1))[0])
    resid = y - model.predict(X[feature_cols].values)
    resid_std = float(np.std(resid, ddof=1)) if len(resid) > 1 else sigma
    return pred, resid_std

def _lstm_predict(x_row, feature_cols, seq_len=20):
    if Sequential is None or LSTM is None:
        return None
    if len(X) <= seq_len + 2:
        return None
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X[feature_cols].values)
    sequences = []
    targets = []
    for i in range(seq_len, len(X_scaled)):
        sequences.append(X_scaled[i - seq_len:i])
        targets.append(y[i])
    X_seq = np.array(sequences)
    y_seq = np.array(targets)
    model = Sequential()
    model.add(LSTM(32, input_shape=(seq_len, X_seq.shape[2])))
    model.add(Dense(1))
    model.compile(optimizer="adam", loss="mse")
    model.fit(X_seq, y_seq, epochs=lstm_epochs, batch_size=32, verbose=0)
    x_last = scaler.transform(x_row[feature_cols].values.reshape(1, -1))
    x_seq = np.repeat(x_last, seq_len, axis=0).reshape(1, seq_len, -1)
    pred = float(model.predict(x_seq, verbose=0)[0][0])
    resid_std = float(np.std(y_seq - model.predict(X_seq, verbose=0).ravel(), ddof=1))
    return pred, resid_std

def _arima_predict():
    model = ARIMA(close, order=(1, 1, 1))
    fit = model.fit()
    steps = max(horizon_days, 1)
    forecast = fit.get_forecast(steps=steps)
    mean_forecast = float(forecast.predicted_mean.iloc[-1])
    resid_std = float(np.std(fit.resid, ddof=1)) if len(fit.resid) > 1 else sigma
    pred_ret = float(np.log(mean_forecast / last_price)) / max(steps, 1)
    return pred_ret, resid_std

feature_cols = list(X.columns)
latest_features = X.iloc[[-1]]

model_results = []

for model_name in models:
    if model_name in ("mean", "gbm"):
        pred_ret, resid_std = _baseline_predict(model_name)
    elif model_name == "ridge":
        out = _ridge_predict(latest_features.iloc[0], feature_cols)
        if out is None:
            missing_notes.append("ridge skipped: dependency missing")
            print("⚠️ Ridge not available; skipping.")
            continue
        pred_ret, resid_std = out
    elif model_name == "xgboost":
        out = _xgb_predict(latest_features.iloc[0], feature_cols)
        if out is None:
            missing_notes.append("xgboost skipped: dependency missing")
            print("⚠️ XGBoost not available; skipping.")
            continue
        pred_ret, resid_std = out
    elif model_name == "lstm":
        out = _lstm_predict(latest_features.iloc[0], feature_cols)
        if out is None:
            missing_notes.append("lstm skipped: dependency missing or insufficient data")
            print("⚠️ LSTM not available or insufficient data; skipping.")
            continue
        pred_ret, resid_std = out
    elif model_name == "arima":
        if ARIMA is None:
            missing_notes.append("arima skipped: statsmodels not available")
            print("⚠️ ARIMA skipped: statsmodels not available; skipping.")
            continue
        if len(close) < 60:
            missing_notes.append("arima skipped: insufficient history (<60)")
            print("⚠️ ARIMA skipped: insufficient history (<60); skipping.")
            continue
        pred_ret, resid_std = _arima_predict()
    elif model_name == "news_event":
        news_cols = [c for c in feature_cols if c.startswith("news_") or c.startswith("earnings_") or c.startswith("analyst_")]
        if not news_cols:
            missing_notes.append("news_event skipped: no news/event features available")
            print("⚠️ No news/event features available; skipping.")
            continue
        out = _ridge_predict(latest_features.iloc[0], news_cols)
        if out is None:
            missing_notes.append("news_event skipped: ridge dependency missing")
            print("⚠️ Ridge not available for news_event; skipping.")
            continue
        pred_ret, resid_std = out
    else:
        print(f"⚠️ Unknown model: {model_name}")
        continue

    expected_price, lower_1s, upper_1s, lower_2s, upper_2s = _price_from_daily_ret(pred_ret)
    model_results.append({
        "model": model_name,
        "pred_ret": pred_ret,
        "resid_std": resid_std,
        "expected_price": expected_price,
        "lower_1s": lower_1s,
        "upper_1s": upper_1s,
        "lower_2s": lower_2s,
        "upper_2s": upper_2s,
    })

if not model_results:
    raise ValueError("No models produced a forecast.")

forecast_tbl = f"{gold_db}.forecast_daily"
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("as_of_date", DateType(), False),
    StructField("target_date", DateType(), False),
    StructField("method", StringType(), False),
    StructField("last_date", DateType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("horizon_days", IntegerType(), True),
    StructField("mu", DoubleType(), True),
    StructField("sigma", DoubleType(), True),
    StructField("expected_price", DoubleType(), True),
    StructField("lower_1s", DoubleType(), True),
    StructField("upper_1s", DoubleType(), True),
    StructField("lower_2s", DoubleType(), True),
    StructField("upper_2s", DoubleType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
])

rows = []
for res in model_results:
    rows.append((
        symbol,
        last_date,
        target_dt,
        res["model"],
        last_date,
        float(last_price),
        int(horizon_days),
        float(res["pred_ret"]),
        float(res["resid_std"]),
        float(res["expected_price"]),
        float(res["lower_1s"]),
        float(res["upper_1s"]),
        float(res["lower_2s"]),
        float(res["upper_2s"]),
        datetime.now(local_tz),
    ))

df = spark.createDataFrame(rows, schema=schema)
if not spark.catalog.tableExists(forecast_tbl):
    df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(forecast_tbl)
else:
    df.createOrReplaceTempView("forecast_updates")
    spark.sql(f"""
        MERGE INTO {forecast_tbl} t
        USING forecast_updates s
        ON t.symbol = s.symbol AND t.target_date = s.target_date AND t.method = s.method
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

if missing_notes:
    print("⚠️ Missing data or skipped models:")
    for note in sorted(set(missing_notes)):
        print(f"  - {note}")
else:
    print("✅ No missing data detected.")

missing_tbl = f"{gold_db}.forecast_missing_inputs"
if missing_notes:
    missing_rows = [
        (run_id, symbol, last_date, target_dt, note, datetime.now(local_tz))
        for note in sorted(set(missing_notes))
    ]
    missing_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("as_of_date", DateType(), False),
        StructField("target_date", DateType(), False),
        StructField("note", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
    ])
    missing_df = spark.createDataFrame(missing_rows, schema=missing_schema)
    if not spark.catalog.tableExists(missing_tbl):
        missing_df.write.mode("overwrite").saveAsTable(missing_tbl)
    else:
        missing_df.write.mode("append").saveAsTable(missing_tbl)

summary_tbl = f"{gold_db}.forecast_run_summary"
duration_seconds = float(time.time() - run_start)
summary_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("as_of_date", DateType(), False),
    StructField("target_date", DateType(), False),
    StructField("mode", StringType(), False),
    StructField("models_requested", StringType(), False),
    StructField("models_completed", StringType(), False),
    StructField("train_days", IntegerType(), False),
    StructField("lstm_epochs", IntegerType(), False),
    StructField("xgb_estimators", IntegerType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])
summary_row = [(
    run_id,
    symbol,
    last_date,
    target_dt,
    mode,
    ",".join(models),
    ",".join([r["model"] for r in model_results]),
    int(train_days),
    int(lstm_epochs),
    int(xgb_estimators),
    duration_seconds,
    datetime.now(local_tz),
)]
summary_df = spark.createDataFrame(summary_row, schema=summary_schema)
if not spark.catalog.tableExists(summary_tbl):
    summary_df.write.mode("overwrite").saveAsTable(summary_tbl)
else:
    summary_df.write.mode("append").saveAsTable(summary_tbl)

dbutils.notebook.exit(json.dumps({"symbol": symbol, "target_date": target_date, "models": [r["model"] for r in model_results]}))
