# Databricks notebook source
# MAGIC %md
# MAGIC # RiskBricks — Register UC Agent Tools
# MAGIC
# MAGIC Registers all Unity Catalog functions in `riskbricks.agent_tools` that power
# MAGIC the RiskBricks AI agent. Each function wraps a gold-layer query and is
# MAGIC callable by the LLM via `UCFunctionToolkit`.
# MAGIC
# MAGIC **Run once** (or re-run to update definitions). Idempotent via `CREATE OR REPLACE`.

# COMMAND ----------

CATALOG = "riskbricks"
SCHEMA  = "agent_tools"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 1 — Portfolio Risk Metrics
# MAGIC Returns VaR, beta, volatility for one or all portfolio managers.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_portfolio_risk(
  p_manager_name STRING COMMENT 'Manager name: "Sarah Russel", "Rena Tang", "Mohit Arora", or "all"'
)
RETURNS TABLE(
  manager_name    STRING,
  risk_profile    STRING,
  aum_usd         DOUBLE,
  portfolio_beta  DOUBLE,
  weighted_volatility_pct DOUBLE,
  var_1day_95_usd DOUBLE,
  var_10day_95_usd DOUBLE,
  num_positions   BIGINT
)
COMMENT 'Returns portfolio-level risk metrics including Value-at-Risk (95% confidence, 1-day and 10-day), portfolio beta, weighted volatility, and position count for one or all managers.'
RETURN
  SELECT
    prm.manager_name,
    prm.risk_profile,
    prm.aum_usd,
    prm.portfolio_beta,
    prm.weighted_volatility_pct,
    prm.var_1day_95_usd,
    prm.var_10day_95_usd,
    prm.num_positions
  FROM {CATALOG}.gold.portfolio_risk_metrics prm
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(prm.manager_name) = LOWER(p_manager_name)
  ORDER BY prm.aum_usd DESC
""")
print("✅ get_portfolio_risk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 2 — Portfolio Holdings
# MAGIC Returns holdings with weight, sector, gain/loss, and risk stats.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_portfolio_holdings(
  p_manager_name STRING COMMENT 'Manager name or "all"',
  p_top_n        INT    COMMENT 'Number of top holdings to return (default 20)' DEFAULT 20
)
RETURNS TABLE(
  manager_name         STRING,
  symbol               STRING,
  sector               STRING,
  weight_pct           DOUBLE,
  value_usd            DOUBLE,
  current_price        DOUBLE,
  unrealized_gain_loss DOUBLE,
  unrealized_gain_loss_pct DOUBLE,
  beta                 DOUBLE,
  volatility_30d       DOUBLE
)
COMMENT 'Returns top portfolio holdings by weight with sector, market value, unrealized P&L, beta, and 30-day volatility for one or all managers.'
RETURN
  SELECT
    pm.manager_name,
    ph.symbol,
    ph.sector,
    ROUND(ph.weight * 100, 2)     AS weight_pct,
    ph.value_usd,
    ph.current_price,
    ph.unrealized_gain_loss,
    ROUND(ph.unrealized_gain_loss_pct, 2) AS unrealized_gain_loss_pct,
    ph.beta,
    ph.volatility_30d
  FROM {CATALOG}.gold.portfolio_holdings ph
  JOIN {CATALOG}.gold.portfolio_managers pm
    ON ph.manager_id = pm.manager_id
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(pm.manager_name) = LOWER(p_manager_name)
  ORDER BY ph.weight DESC
  LIMIT p_top_n
""")
print("✅ get_portfolio_holdings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 3 — Sector Exposure
# MAGIC Returns sector allocation percentages by manager.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_sector_exposure(
  p_manager_name STRING COMMENT 'Manager name or "all"'
)
RETURNS TABLE(
  manager_name      STRING,
  sector            STRING,
  sector_weight_pct DOUBLE
)
COMMENT 'Returns sector allocation breakdown (percentage weights) for one or all managers. Useful for concentration risk and diversification analysis.'
RETURN
  SELECT
    se.manager_name,
    se.sector,
    ROUND(se.sector_weight_pct, 2) AS sector_weight_pct
  FROM {CATALOG}.gold.sector_exposures se
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(se.manager_name) = LOWER(p_manager_name)
  ORDER BY se.manager_name, se.sector_weight_pct DESC
""")
print("✅ get_sector_exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 4 — Stress Test Results
# MAGIC Returns scenario impacts (market crash, tech drawdown, rate spike, recession).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_stress_tests(
  p_manager_name STRING COMMENT 'Manager name or "all"'
)
RETURNS TABLE(
  manager_name          STRING,
  scenario_name         STRING,
  scenario_description  STRING,
  aum_usd               DOUBLE,
  total_impact_usd      DOUBLE,
  impact_pct            DOUBLE
)
COMMENT 'Returns stress test results for 4 scenarios (Market Crash -20%, Tech Drawdown -30%, Rate Spike, Recession). Shows estimated USD and percentage portfolio impact.'
RETURN
  SELECT
    st.manager_name,
    st.scenario_name,
    st.scenario_description,
    st.aum_usd,
    st.total_impact_usd,
    ROUND(st.impact_pct, 2) AS impact_pct
  FROM {CATALOG}.gold.stress_test_results st
  WHERE LOWER(p_manager_name) = 'all'
     OR LOWER(st.manager_name) = LOWER(p_manager_name)
  ORDER BY st.manager_name, ABS(st.impact_pct) DESC
""")
print("✅ get_stress_tests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 5 — Stock Forecasts
# MAGIC Returns price forecasts with confidence bands for a symbol.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_stock_forecast(
  p_symbol STRING COMMENT 'Ticker symbol (e.g. NVDA, AAPL) or "all"'
)
RETURNS TABLE(
  symbol              STRING,
  as_of_date          DATE,
  last_close          DOUBLE,
  forecast_date       DATE,
  horizon_days        INT,
  predicted_price     DOUBLE,
  predicted_direction STRING,
  confidence_band_low DOUBLE,
  confidence_band_high DOUBLE,
  top_factors         ARRAY<STRING>
)
COMMENT 'Returns stock price forecasts with predicted price, direction (UP/DOWN/FLAT), confidence bands, and top driving factors. Multiple horizons per symbol (7d, 30d, 90d).'
RETURN
  SELECT
    sf.symbol,
    sf.as_of_date,
    sf.last_close,
    sf.forecast_date,
    sf.horizon_days,
    sf.predicted_price,
    sf.predicted_direction,
    sf.confidence_band_low,
    sf.confidence_band_high,
    sf.top_factors
  FROM {CATALOG}.gold.stock_forecasts sf
  WHERE LOWER(p_symbol) = 'all'
     OR UPPER(sf.symbol) = UPPER(p_symbol)
  ORDER BY sf.symbol, sf.horizon_days
""")
print("✅ get_stock_forecast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 6 — Stock Risk Metrics
# MAGIC Returns per-stock volatility, beta, VaR, drawdown.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_stock_risk(
  p_symbol STRING COMMENT 'Ticker symbol or "all"'
)
RETURNS TABLE(
  symbol       STRING,
  as_of_date   DATE,
  ewma_vol     DOUBLE,
  vol_20d      DOUBLE,
  vol_252d     DOUBLE,
  beta_1y      DOUBLE,
  max_drawdown DOUBLE,
  var_95       DOUBLE,
  es_95        DOUBLE
)
COMMENT 'Returns per-stock risk metrics: EWMA and realized volatility, 1-year beta to SPY, max drawdown, Value-at-Risk and Expected Shortfall at 95% confidence.'
RETURN
  SELECT
    r.symbol,
    r.as_of_date,
    r.ewma_vol,
    r.vol_20d,
    r.vol_252d,
    r.beta_1y,
    r.max_drawdown,
    r.var_95,
    r.es_95
  FROM {CATALOG}.gold.risk_metrics_daily r
  WHERE LOWER(p_symbol) = 'all'
     OR UPPER(r.symbol) = UPPER(p_symbol)
  ORDER BY r.as_of_date DESC
  LIMIT 50
""")
print("✅ get_stock_risk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 7 — Macro Economic Context
# MAGIC Returns the latest macro indicators (Fed rate, CPI, unemployment, VIX, etc.).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_macro_context()
RETURNS TABLE(
  indicator_name STRING,
  latest_date    DATE,
  latest_value   DOUBLE
)
COMMENT 'Returns latest macroeconomic indicators: Federal Funds Rate, CPI, Unemployment Rate, GDP Growth, VIX, 10Y Treasury Yield, and others from FRED data.'
RETURN
  SELECT
    m.indicator_name,
    m.date AS latest_date,
    m.value AS latest_value
  FROM {CATALOG}.silver.macro_indicators m
  INNER JOIN (
    SELECT indicator_name, MAX(date) AS max_date
    FROM {CATALOG}.silver.macro_indicators
    WHERE is_valid = true
    GROUP BY indicator_name
  ) latest
    ON m.indicator_name = latest.indicator_name
   AND m.date = latest.max_date
  ORDER BY m.indicator_name
""")
print("✅ get_macro_context")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 8 — Decision Signals (Buy/Hold/Sell)
# MAGIC Returns the latest trade signal for a symbol.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_decision_signal(
  p_symbol STRING COMMENT 'Ticker symbol or "all"'
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
COMMENT 'Returns the latest Buy/Hold/Sell decision signal with confidence score, expected return, model consensus count, and risk context.'
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
  FROM {CATALOG}.gold.decision_signals d
  INNER JOIN (
    SELECT symbol, MAX(as_of_date) AS max_date
    FROM {CATALOG}.gold.decision_signals
    GROUP BY symbol
  ) latest
    ON d.symbol = latest.symbol AND d.as_of_date = latest.max_date
  WHERE LOWER(p_symbol) = 'all'
     OR UPPER(d.symbol) = UPPER(p_symbol)
  ORDER BY d.symbol
""")
print("✅ get_decision_signal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 9 — Company Lookup
# MAGIC Returns company info from the universe table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_company_info(
  p_symbol STRING COMMENT 'Ticker symbol or sector name to search'
)
RETURNS TABLE(
  symbol          STRING,
  company_name    STRING,
  sector          STRING,
  industry        STRING,
  beta            DOUBLE,
  volatility_30d  DOUBLE,
  latest_price    DOUBLE,
  market_cap_usd  DOUBLE,
  pe_ratio        DOUBLE,
  is_sp500        BOOLEAN
)
COMMENT 'Returns company details from the 432-company universe: name, sector, industry, beta, volatility, price, market cap, P/E ratio, and S&P 500 membership. Can also search by sector.'
RETURN
  SELECT
    c.symbol,
    c.company_name,
    c.sector,
    c.industry,
    c.beta,
    c.volatility_30d,
    c.latest_price,
    c.market_cap_usd,
    c.pe_ratio,
    c.is_sp500
  FROM {CATALOG}.gold.company_universe c
  WHERE UPPER(c.symbol) = UPPER(p_symbol)
     OR LOWER(c.sector) = LOWER(p_symbol)
  ORDER BY c.market_cap_usd DESC NULLS LAST
  LIMIT 50
""")
print("✅ get_company_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 10 — Geopolitical Risk Events
# MAGIC Returns active geopolitical risk events and their market impact.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_geopolitical_risks()
RETURNS TABLE(
  event_name                  STRING,
  event_category              STRING,
  severity                    BIGINT,
  estimated_market_impact_pct DOUBLE,
  affected_sectors            STRING,
  event_date                  TIMESTAMP,
  description                 STRING
)
COMMENT 'Returns active geopolitical risk events with severity rating, estimated market impact percentage, affected sectors, and description. Useful for scenario analysis.'
RETURN
  SELECT
    g.event_name,
    g.event_category,
    g.severity,
    g.estimated_market_impact_pct,
    g.affected_sectors,
    g.event_date,
    g.description
  FROM {CATALOG}.gold.geopolitical_risk_events g
  WHERE g.is_active = true
  ORDER BY g.severity DESC, ABS(g.estimated_market_impact_pct) DESC
""")
print("✅ get_geopolitical_risks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool 11 — Factor Exposures (Fama-French)
# MAGIC Returns Fama-French 3-factor model exposures for a stock.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_factor_exposure(
  p_symbol STRING COMMENT 'Ticker symbol or "all"'
)
RETURNS TABLE(
  symbol          STRING,
  model           STRING,
  alpha           DOUBLE,
  beta_mkt        DOUBLE,
  beta_smb        DOUBLE,
  beta_hml        DOUBLE,
  factor_var      DOUBLE,
  idio_var        DOUBLE,
  annualized_vol  DOUBLE
)
COMMENT 'Returns Fama-French 3-factor model exposures: market beta, size factor (SMB), value factor (HML), alpha, factor/idiosyncratic variance decomposition, and annualized volatility.'
RETURN
  SELECT
    r.symbol,
    r.model,
    r.alpha,
    r.beta_mkt,
    r.beta_smb,
    r.beta_hml,
    r.factor_var,
    r.idio_var,
    r.annualized_vol
  FROM {CATALOG}.gold.risk_factor_exposures r
  WHERE LOWER(p_symbol) = 'all'
     OR UPPER(r.symbol) = UPPER(p_symbol)
  ORDER BY r.computed_at DESC
  LIMIT 50
""")
print("✅ get_factor_exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

tools = spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.{SCHEMA}").filter("function LIKE 'get_%'")
print(f"\n{'='*60}")
print(f"✅ {tools.count()} UC tools registered in {CATALOG}.{SCHEMA}")
print(f"{'='*60}")
display(tools)
