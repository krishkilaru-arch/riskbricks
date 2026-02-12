# Databricks notebook source
# MAGIC %md
# MAGIC # 🧰 Register UC Tools (SQL Functions)
# MAGIC
# MAGIC Creates Unity Catalog SQL functions under `riskbricks.tools` that power
# MAGIC Mosaic AI agents and tool calls.

# COMMAND ----------

catalog = "riskbricks"
tools_schema = f"{catalog}.tools"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tools_schema}")

# Create minimal tables if missing so function registration succeeds.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.forecast_daily (
  symbol STRING,
  target_date DATE,
  method STRING,
  expected_price DOUBLE,
  lower_1s DOUBLE,
  upper_1s DOUBLE,
  lower_2s DOUBLE,
  upper_2s DOUBLE,
  last_price DOUBLE,
  ingestion_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.risk_metrics_daily (
  symbol STRING,
  as_of_date DATE,
  ewma_vol DOUBLE,
  vol_20d DOUBLE,
  vol_60d DOUBLE,
  vol_252d DOUBLE,
  beta_1y DOUBLE,
  max_drawdown DOUBLE,
  var_95 DOUBLE,
  es_95 DOUBLE,
  adv_20d DOUBLE,
  impact DOUBLE,
  ingestion_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.decision_signals (
  symbol STRING,
  as_of_date DATE,
  target_date DATE,
  signal STRING,
  score DOUBLE,
  expected_return DOUBLE,
  model_count INT,
  ingestion_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.rag_corpus (
  symbol STRING,
  published_date DATE,
  title STRING,
  source STRING,
  url STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.news_impact_history (
  symbol STRING,
  impact_1d_pct DOUBLE
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.risk_factor_exposures (
  symbol STRING,
  model STRING,
  alpha DOUBLE,
  beta_mkt DOUBLE,
  beta_smb DOUBLE,
  beta_hml DOUBLE,
  factor_var DOUBLE,
  idio_var DOUBLE,
  total_var DOUBLE,
  annualized_vol DOUBLE,
  start_date DATE,
  end_date DATE,
  computed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.forecast_eval_daily (
  symbol STRING,
  target_date DATE,
  method STRING,
  predicted_price DOUBLE,
  actual_price DOUBLE,
  mape DOUBLE,
  ingestion_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.geopolitical_risk_events (
  event_name STRING,
  event_category STRING,
  severity STRING,
  estimated_market_impact_pct DOUBLE,
  affected_sectors STRING,
  event_date DATE,
  is_active BOOLEAN
) USING DELTA
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_latest_forecast(p_symbol STRING, p_target_date DATE)
RETURNS STRING
RETURN (
  WITH params AS (SELECT p_symbol AS symbol, p_target_date AS target_date)
  SELECT to_json(named_struct(
    'symbol', t.symbol,
    'target_date', t.target_date,
    'method', t.method,
    'expected_price', t.expected_price,
    'lower_1s', t.lower_1s,
    'upper_1s', t.upper_1s,
    'lower_2s', t.lower_2s,
    'upper_2s', t.upper_2s,
    'last_price', t.last_price,
    'ingestion_timestamp', t.ingestion_timestamp
  ))
  FROM {catalog}.gold.forecast_daily t
  CROSS JOIN params p
  WHERE t.symbol = p.symbol AND t.target_date = p.target_date
  ORDER BY t.ingestion_timestamp DESC
  LIMIT 1
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_risk_metrics(p_symbol STRING, p_as_of_date DATE)
RETURNS STRING
RETURN (
  WITH params AS (SELECT p_symbol AS symbol, p_as_of_date AS as_of_date)
  SELECT to_json(named_struct(
    'symbol', t.symbol,
    'as_of_date', t.as_of_date,
    'ewma_vol', t.ewma_vol,
    'vol_20d', t.vol_20d,
    'vol_60d', t.vol_60d,
    'vol_252d', t.vol_252d,
    'beta_1y', t.beta_1y,
    'max_drawdown', t.max_drawdown,
    'var_95', t.var_95,
    'es_95', t.es_95,
    'adv_20d', t.adv_20d,
    'impact', t.impact,
    'ingestion_timestamp', t.ingestion_timestamp
  ))
  FROM {catalog}.gold.risk_metrics_daily t
  CROSS JOIN params p
  WHERE t.symbol = p.symbol
    AND t.as_of_date = COALESCE(
      p.as_of_date,
      (SELECT MAX(as_of_date) FROM {catalog}.gold.risk_metrics_daily WHERE symbol = p.symbol)
    )
  ORDER BY t.ingestion_timestamp DESC
  LIMIT 1
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_decision_signal(p_symbol STRING, p_as_of_date DATE)
RETURNS STRING
RETURN (
  WITH params AS (SELECT p_symbol AS symbol, p_as_of_date AS as_of_date)
  SELECT to_json(named_struct(
    'symbol', t.symbol,
    'as_of_date', t.as_of_date,
    'target_date', t.target_date,
    'signal', t.signal,
    'score', t.score,
    'expected_return', t.expected_return,
    'model_count', t.model_count
  ))
  FROM {catalog}.gold.decision_signals t
  CROSS JOIN params p
  WHERE t.symbol = p.symbol
    AND t.as_of_date = COALESCE(
      p.as_of_date,
      (SELECT MAX(as_of_date) FROM {catalog}.gold.decision_signals WHERE symbol = p.symbol)
    )
  ORDER BY t.ingestion_timestamp DESC
  LIMIT 1
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_news_context(p_symbol STRING, p_days INT)
RETURNS STRING
RETURN (
  WITH params AS (SELECT p_symbol AS symbol, p_days AS days)
  SELECT to_json(collect_list(named_struct(
    'published_date', published_date,
    'title', title,
    'source', source,
    'url', url
  )))
  FROM (
    SELECT rc.published_date, rc.title, rc.source, rc.url
    FROM {catalog}.gold.rag_corpus rc
    CROSS JOIN params p
    WHERE rc.symbol = p.symbol
      AND rc.published_date >= date_sub(current_date(), COALESCE(p.days, 7))
    ORDER BY rc.published_date DESC
    LIMIT 20
  )
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_news_impact_stats(p_symbol STRING)
RETURNS STRING
RETURN (
  SELECT to_json(named_struct(
    'symbol', p_symbol,
    'impact_1d_avg', s.impact_1d_avg,
    'impact_1d_abs_avg', s.impact_1d_abs_avg,
    'event_count', s.event_count
  ))
  FROM (
    SELECT
      avg(impact_1d_pct) AS impact_1d_avg,
      avg(abs(impact_1d_pct)) AS impact_1d_abs_avg,
      count(*) AS event_count
    FROM {catalog}.gold.news_impact_history
    WHERE symbol = p_symbol
  ) s
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_factor_exposures(p_symbol STRING)
RETURNS STRING
RETURN (
  WITH params AS (SELECT p_symbol AS symbol)
  SELECT to_json(named_struct(
    'symbol', t.symbol,
    'model', t.model,
    'alpha', t.alpha,
    'beta_mkt', t.beta_mkt,
    'beta_smb', t.beta_smb,
    'beta_hml', t.beta_hml,
    'factor_var', t.factor_var,
    'idio_var', t.idio_var,
    'total_var', t.total_var,
    'annualized_vol', t.annualized_vol,
    'start_date', t.start_date,
    'end_date', t.end_date,
    'computed_at', t.computed_at
  ))
  FROM {catalog}.gold.risk_factor_exposures t
  CROSS JOIN params p
  WHERE t.symbol = p.symbol
  ORDER BY t.computed_at DESC
  LIMIT 1
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_forecast_eval(p_symbol STRING, p_target_date DATE)
RETURNS STRING
RETURN (
  WITH params AS (SELECT p_symbol AS symbol, p_target_date AS target_date)
  SELECT to_json(collect_list(named_struct(
    'symbol', t.symbol,
    'target_date', t.target_date,
    'method', t.method,
    'predicted_price', t.predicted_price,
    'actual_price', t.actual_price,
    'mape', t.mape,
    'ingestion_timestamp', t.ingestion_timestamp
  )))
  FROM {catalog}.gold.forecast_eval_daily t
  CROSS JOIN params p
  WHERE t.symbol = p.symbol AND t.target_date = p.target_date
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_geopolitical_events()
RETURNS STRING
RETURN (
  SELECT to_json(collect_list(named_struct(
    'event_name', event_name,
    'event_category', event_category,
    'severity', severity,
    'estimated_market_impact_pct', estimated_market_impact_pct,
    'affected_sectors', affected_sectors,
    'event_date', event_date
  )))
  FROM {catalog}.gold.geopolitical_risk_events
  WHERE is_active = true
)
""")

print("✅ UC tools created in riskbricks.tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

