# Databricks notebook source
# MAGIC %md
# MAGIC # Register UC Tools for RiskBricks Agent
# MAGIC
# MAGIC Creates Unity Catalog functions in `riskbricks.agent_tools` that the LLM agent
# MAGIC can call as tools via `UCFunctionToolkit`.
# MAGIC
# MAGIC **Run once** (or re-run to update). Idempotent via CREATE OR REPLACE.

# COMMAND ----------

dbutils.widgets.text("catalog", "riskbricks")
catalog = dbutils.widgets.get("catalog").strip()
schema  = "agent_tools"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
print(f"✅ Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 1: Portfolio Risk Metrics
# MAGIC Returns VaR, beta, volatility for one or all portfolio managers.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_portfolio_risk_metrics(
  p_manager_name STRING COMMENT 'Manager name (e.g. "Sarah Russel", "Rena Tang", "Mohit Arora") or "all"'
)
RETURNS TABLE(
  manager_name      STRING,
  risk_profile      STRING,
  aum_usd           DOUBLE,
  portfolio_beta    DOUBLE,
  weighted_volatility_pct DOUBLE,
  var_1day_95_usd   DOUBLE,
  var_10day_95_usd  DOUBLE,
  num_positions     BIGINT
)
COMMENT 'Returns portfolio risk metrics including Value-at-Risk (95% confidence), beta, and volatility for a portfolio manager. Use "all" to compare all managers.'
RETURN
  SELECT
    r.manager_name,
    r.risk_profile,
    r.aum_usd,
    r.portfolio_beta,
    r.weighted_volatility_pct,
    r.var_1day_95_usd,
    r.var_10day_95_usd,
    r.num_positions
  FROM {catalog}.gold.portfolio_risk_metrics r
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(r.manager_name) = LOWER(p_manager_name)
""")
print("✅ get_portfolio_risk_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 2: Stress Test Results
# MAGIC Returns scenario impacts (Market Crash, Tech Drawdown, Rate Spike, Recession).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_stress_test_results(
  p_manager_name STRING COMMENT 'Manager name or "all"'
)
RETURNS TABLE(
  manager_name         STRING,
  scenario_name        STRING,
  scenario_description STRING,
  impact_usd           DOUBLE,
  impact_pct           DOUBLE
)
COMMENT 'Returns stress test results showing portfolio impact under 4 scenarios: Market Crash (-20%), Tech Drawdown (-30%), Rate Spike (+200bp), Recession (-15%). Use "all" for all managers.'
RETURN
  SELECT
    s.manager_name,
    s.scenario_name,
    s.scenario_description,
    s.total_impact_usd AS impact_usd,
    s.impact_pct
  FROM {catalog}.gold.stress_test_results s
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(s.manager_name) = LOWER(p_manager_name)
  ORDER BY s.manager_name, s.scenario_name
""")
print("✅ get_stress_test_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 3: Portfolio Holdings
# MAGIC Returns individual positions with weight, value, P&L.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_portfolio_holdings(
  p_manager_name STRING COMMENT 'Manager name or "all"'
)
RETURNS TABLE(
  manager_name          STRING,
  risk_profile          STRING,
  symbol                STRING,
  sector                STRING,
  weight_pct            DOUBLE,
  value_usd             DOUBLE,
  current_price         DOUBLE,
  unrealized_gain_loss_pct DOUBLE,
  beta                  DOUBLE,
  volatility_30d        DOUBLE
)
COMMENT 'Returns portfolio holdings with weight, current value, unrealized P&L, and risk stats. Returns top 25 positions by weight. Use "all" for all managers.'
RETURN
  SELECT
    pm.manager_name,
    pm.risk_profile,
    h.symbol,
    h.sector,
    ROUND(h.weight * 100, 2)    AS weight_pct,
    h.value_usd,
    h.current_price,
    ROUND(h.unrealized_gain_loss_pct, 2) AS unrealized_gain_loss_pct,
    h.beta,
    h.volatility_30d
  FROM {catalog}.gold.portfolio_holdings h
  JOIN {catalog}.gold.portfolio_managers pm ON h.manager_id = pm.manager_id
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(pm.manager_name) = LOWER(p_manager_name)
  ORDER BY h.weight DESC
  LIMIT 25
""")
print("✅ get_portfolio_holdings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 4: Sector Exposures
# MAGIC Returns sector allocation breakdown by manager.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_sector_exposures(
  p_manager_name STRING COMMENT 'Manager name or "all"'
)
RETURNS TABLE(
  manager_name     STRING,
  sector           STRING,
  sector_weight_pct DOUBLE
)
COMMENT 'Returns sector allocation percentages for a portfolio manager. Shows how concentrated a portfolio is across sectors like Technology, Healthcare, Finance, etc.'
RETURN
  SELECT
    s.manager_name,
    s.sector,
    s.sector_weight_pct
  FROM {catalog}.gold.sector_exposures s
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(s.manager_name) = LOWER(p_manager_name)
  ORDER BY s.manager_name, s.sector_weight_pct DESC
""")
print("✅ get_sector_exposures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 5: Macro Economic Context
# MAGIC Returns latest macro indicators (Fed Funds Rate, CPI, GDP, Unemployment, VIX).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_macro_context()
RETURNS TABLE(
  indicator_name STRING,
  latest_value   DOUBLE,
  as_of_date     DATE,
  units          STRING
)
COMMENT 'Returns the latest macroeconomic indicators: Federal Funds Rate, CPI, GDP Growth, Unemployment Rate, S&P 500 level, and VIX. Useful for understanding the current macro environment and its impact on portfolios.'
RETURN
  SELECT
    m.indicator_name,
    m.value AS latest_value,
    m.date  AS as_of_date,
    m.units
  FROM {catalog}.gold.macro_indicators_daily m
  INNER JOIN (
    SELECT indicator_name, MAX(date) AS max_date
    FROM {catalog}.gold.macro_indicators_daily
    GROUP BY indicator_name
  ) latest ON m.indicator_name = latest.indicator_name AND m.date = latest.max_date
  ORDER BY m.indicator_name
""")
print("✅ get_macro_context")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 6: Stock Forecasts
# MAGIC Returns price forecasts with confidence bands.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_stock_forecast(
  p_symbol STRING COMMENT 'Stock ticker symbol (e.g. "NVDA", "AAPL")'
)
RETURNS TABLE(
  symbol           STRING,
  as_of_date       DATE,
  forecast_date    DATE,
  horizon_days     INT,
  last_close       DOUBLE,
  predicted_price  DOUBLE,
  predicted_direction STRING,
  confidence_low   DOUBLE,
  confidence_high  DOUBLE,
  top_factors      ARRAY<STRING>
)
COMMENT 'Returns stock price forecasts at 1-day, 5-day, and 15-day horizons with confidence bands and top driving factors.'
RETURN
  SELECT
    f.symbol,
    f.as_of_date,
    f.forecast_date,
    f.horizon_days,
    f.last_close,
    f.predicted_price,
    f.predicted_direction,
    f.confidence_band_low  AS confidence_low,
    f.confidence_band_high AS confidence_high,
    f.top_factors
  FROM {catalog}.gold.stock_forecasts f
  WHERE UPPER(f.symbol) = UPPER(p_symbol)
  ORDER BY f.horizon_days
""")
print("✅ get_stock_forecast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 7: Decision Signals
# MAGIC Returns Buy/Hold/Sell signals with composite scores.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_decision_signal(
  p_symbol STRING COMMENT 'Stock ticker symbol (e.g. "NVDA", "AAPL") or "all"'
)
RETURNS TABLE(
  symbol          STRING,
  as_of_date      DATE,
  signal          STRING,
  score           DOUBLE,
  expected_return DOUBLE,
  model_count     BIGINT,
  vol_20d         DOUBLE,
  beta_1y         DOUBLE
)
COMMENT 'Returns the latest Buy/Hold/Sell decision signal for a stock with composite score (0-100), expected return, and key risk metrics. Use "all" for all signals.'
RETURN
  SELECT
    d.symbol,
    d.as_of_date,
    d.signal,
    d.score,
    d.expected_return,
    d.model_count,
    d.vol_20d,
    d.beta_1y
  FROM {catalog}.gold.decision_signals d
  WHERE UPPER(p_symbol) = 'ALL'
     OR UPPER(d.symbol) = UPPER(p_symbol)
  ORDER BY d.score DESC
""")
print("✅ get_decision_signal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 8: Factor Exposures (Fama-French)
# MAGIC Returns market/SMB/HML factor betas for a stock.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_factor_exposures(
  p_symbol STRING COMMENT 'Stock ticker symbol (e.g. "NVDA", "AAPL")'
)
RETURNS TABLE(
  symbol         STRING,
  model          STRING,
  alpha          DOUBLE,
  beta_mkt       DOUBLE,
  beta_smb       DOUBLE,
  beta_hml       DOUBLE,
  annualized_vol DOUBLE,
  start_date     STRING,
  end_date       STRING
)
COMMENT 'Returns Fama-French 3-factor model exposures: market beta, size (SMB), and value (HML) factor loadings plus annualized volatility. Useful for understanding systematic risk drivers.'
RETURN
  SELECT
    r.symbol,
    r.model,
    r.alpha,
    r.beta_mkt,
    r.beta_smb,
    r.beta_hml,
    r.annualized_vol,
    r.start_date,
    r.end_date
  FROM {catalog}.gold.risk_factor_exposures r
  WHERE UPPER(r.symbol) = UPPER(p_symbol)
  ORDER BY r.computed_at DESC
  LIMIT 1
""")
print("✅ get_factor_exposures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Tools

# COMMAND ----------

tools = spark.sql(f"SHOW FUNCTIONS IN {catalog}.{schema}").collect()
print(f"\n{'='*60}")
print(f"✅ {len(tools)} UC tools registered in {catalog}.{schema}:")
for t in tools:
    print(f"   • {t.function}")
print(f"{'='*60}")

# COMMAND ----------

# Quick smoke test
print("\n🔍 Smoke tests:")
r = spark.sql(f"SELECT * FROM {catalog}.{schema}.get_portfolio_risk_metrics('all')").count()
print(f"  get_portfolio_risk_metrics('all') → {r} rows")

r = spark.sql(f"SELECT * FROM {catalog}.{schema}.get_stress_test_results('Sarah Russel')").count()
print(f"  get_stress_test_results('Sarah Russel') → {r} rows")

r = spark.sql(f"SELECT * FROM {catalog}.{schema}.get_macro_context()").count()
print(f"  get_macro_context() → {r} rows")

r = spark.sql(f"SELECT * FROM {catalog}.{schema}.get_stock_forecast('NVDA')").count()
print(f"  get_stock_forecast('NVDA') → {r} rows")

r = spark.sql(f"SELECT * FROM {catalog}.{schema}.get_decision_signal('NVDA')").count()
print(f"  get_decision_signal('NVDA') → {r} rows")

print("\n✅ All smoke tests passed")
