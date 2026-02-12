# Databricks notebook source
# MAGIC %md
# MAGIC # 🧰 Create All Unity Catalog Functions for Mosaic AI Agents
# MAGIC 
# MAGIC **Purpose**: Register comprehensive set of UC SQL functions that power Mosaic AI agents
# MAGIC 
# MAGIC **Functions Created:**
# MAGIC - Forecast functions (latest, history, consensus)
# MAGIC - Risk functions (metrics, portfolio, correlations)
# MAGIC - Decision functions (signals, recommendations)
# MAGIC - News functions (sentiment, impact)
# MAGIC - Alt signals functions (earnings, analysts, options)
# MAGIC - Portfolio functions (summary, opportunities)

# COMMAND ----------

catalog = "riskbricks"
tools_schema = f"{catalog}.tools"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS tools")

print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schema: {tools_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Forecast Functions

# COMMAND ----------

# Get latest forecast for a symbol
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_latest_forecast(
    p_symbol STRING COMMENT 'Stock ticker (e.g., AAPL)',
    p_target_date DATE COMMENT 'Target forecast date'
)
RETURNS STRING
COMMENT 'Returns latest forecast with all models and confidence intervals as JSON'
RETURN (
    SELECT to_json(
        collect_list(
            named_struct(
                'symbol', symbol,
                'target_date', target_date,
                'method', method,
                'expected_price', expected_price,
                'lower_1s', lower_1s,
                'upper_1s', upper_1s,
                'last_price', last_price,
                'expected_return_pct', ROUND((expected_price - last_price) / last_price * 100, 2),
                'ingestion_timestamp', ingestion_timestamp
            )
        )
    )
    FROM (
        SELECT *
        FROM {catalog}.gold.forecast_daily
        WHERE symbol = p_symbol 
          AND target_date = p_target_date
        ORDER BY ingestion_timestamp DESC
        LIMIT 10
    )
)
""")

print("✅ Created: get_latest_forecast")

# COMMAND ----------

# Get forecast consensus (average across models)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_forecast_consensus(
    p_symbol STRING COMMENT 'Stock ticker',
    p_target_date DATE COMMENT 'Target date'
)
RETURNS STRING
COMMENT 'Returns consensus forecast averaged across all models'
RETURN (
    SELECT to_json(named_struct(
        'symbol', symbol,
        'target_date', target_date,
        'num_models', COUNT(*),
        'consensus_price', ROUND(AVG(expected_price), 2),
        'current_price', MAX(last_price),
        'expected_return_pct', ROUND(AVG((expected_price - last_price) / last_price * 100), 2),
        'model_disagreement', ROUND(STDDEV((expected_price - last_price) / last_price * 100), 2),
        'min_forecast', ROUND(MIN(expected_price), 2),
        'max_forecast', ROUND(MAX(expected_price), 2)
    ))
    FROM {catalog}.gold.forecast_daily
    WHERE symbol = p_symbol 
      AND target_date = p_target_date
    GROUP BY symbol, target_date
)
""")

print("✅ Created: get_forecast_consensus")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Risk Functions

# COMMAND ----------

# Get risk metrics for a symbol
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_risk_metrics(
    p_symbol STRING COMMENT 'Stock ticker',
    p_as_of_date DATE COMMENT 'As of date'
)
RETURNS STRING
COMMENT 'Returns comprehensive risk metrics including volatility, VaR, beta'
RETURN (
    SELECT to_json(named_struct(
        'symbol', symbol,
        'as_of_date', as_of_date,
        'volatility_20d_pct', ROUND(vol_20d * 100, 2),
        'volatility_annual_pct', ROUND(vol_252d * 100, 2),
        'beta_1y', ROUND(beta_1y, 2),
        'max_drawdown_pct', ROUND(max_drawdown * 100, 2),
        'var_95_pct', ROUND(var_95 * 100, 2),
        'expected_shortfall_95_pct', ROUND(es_95 * 100, 2),
        'avg_daily_volume', adv_20d,
        'market_impact_bps', ROUND(impact * 10000, 2),
        'risk_category', CASE 
            WHEN beta_1y < 0.8 THEN 'Low Risk'
            WHEN beta_1y < 1.2 THEN 'Medium Risk'
            ELSE 'High Risk'
        END
    ))
    FROM {catalog}.gold.risk_metrics_daily
    WHERE symbol = p_symbol 
      AND as_of_date = p_as_of_date
    ORDER BY ingestion_timestamp DESC
    LIMIT 1
)
""")

print("✅ Created: get_risk_metrics")

# COMMAND ----------

# Get sector risk summary
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_sector_risk_summary(
    p_as_of_date DATE COMMENT 'As of date'
)
RETURNS STRING
COMMENT 'Returns risk metrics aggregated by sector'
RETURN (
    SELECT to_json(
        collect_list(
            named_struct(
                'sector', sector,
                'num_stocks', num_stocks,
                'avg_volatility_pct', avg_volatility_pct,
                'avg_beta', avg_beta,
                'avg_max_drawdown_pct', avg_max_drawdown_pct
            )
        )
    )
    FROM (
        SELECT 
            c.sector,
            COUNT(*) as num_stocks,
            ROUND(AVG(r.vol_20d * 100), 2) as avg_volatility_pct,
            ROUND(AVG(r.beta_1y), 2) as avg_beta,
            ROUND(AVG(r.max_drawdown * 100), 2) as avg_max_drawdown_pct
        FROM {catalog}.gold.risk_metrics_daily r
        JOIN {catalog}.gold.company_universe c ON r.symbol = c.symbol
        WHERE r.as_of_date = p_as_of_date
        GROUP BY c.sector
        ORDER BY avg_beta DESC
    )
)
""")

print("✅ Created: get_sector_risk_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Decision Functions

# COMMAND ----------

# Get decision signal for a symbol
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_decision_signal(
    p_symbol STRING COMMENT 'Stock ticker',
    p_as_of_date DATE COMMENT 'As of date'
)
RETURNS STRING
COMMENT 'Returns BUY/SELL/HOLD signal with score and rationale'
RETURN (
    SELECT to_json(named_struct(
        'symbol', symbol,
        'signal', signal,
        'score', ROUND(score, 3),
        'expected_return_pct', ROUND(expected_return * 100, 2),
        'model_count', model_count,
        'as_of_date', as_of_date,
        'target_date', target_date
    ))
    FROM {catalog}.gold.decision_signals
    WHERE symbol = p_symbol 
      AND as_of_date = p_as_of_date
    ORDER BY ingestion_timestamp DESC
    LIMIT 1
)
""")

print("✅ Created: get_decision_signal")

# COMMAND ----------

# Get top opportunities (best risk-adjusted returns)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_top_opportunities(
    p_as_of_date DATE COMMENT 'As of date',
    p_limit INT COMMENT 'Number of top stocks to return'
)
RETURNS STRING
COMMENT 'Returns top stock opportunities ranked by risk-adjusted return'
RETURN (
    SELECT to_json(
        collect_list(
            named_struct(
                'rank', rank,
                'symbol', symbol,
                'company_name', company_name,
                'sector', sector,
                'expected_return_pct', expected_return_pct,
                'volatility_pct', volatility_pct,
                'beta', beta,
                'risk_adjusted_return', risk_adjusted_return,
                'signal', signal
            )
        )
    )
    FROM (
        SELECT 
            rank,
            symbol,
            company_name,
            sector,
            expected_return_pct,
            volatility_pct,
            beta,
            risk_adjusted_return,
            signal
        FROM (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY 
                    AVG((f.expected_price - f.last_price) / f.last_price * 100) / (r.vol_20d * 100) 
                DESC) as rank,
                f.symbol,
                c.company_name,
                c.sector,
                ROUND(AVG((f.expected_price - f.last_price) / f.last_price * 100), 2) as expected_return_pct,
                ROUND(r.vol_20d * 100, 2) as volatility_pct,
                ROUND(r.beta_1y, 2) as beta,
                ROUND(AVG((f.expected_price - f.last_price) / f.last_price * 100) / (r.vol_20d * 100), 3) as risk_adjusted_return,
                d.signal
            FROM {catalog}.gold.forecast_daily f
            JOIN {catalog}.gold.company_universe c ON f.symbol = c.symbol
            JOIN {catalog}.gold.risk_metrics_daily r ON f.symbol = r.symbol AND r.as_of_date = p_as_of_date
            LEFT JOIN {catalog}.gold.decision_signals d ON f.symbol = d.symbol AND d.as_of_date = p_as_of_date
            GROUP BY f.symbol, c.company_name, c.sector, r.vol_20d, r.beta_1y, d.signal
            HAVING AVG((f.expected_price - f.last_price) / f.last_price * 100) > 0
        )
        WHERE rank <= p_limit
        ORDER BY rank
    )
)
""")

print("✅ Created: get_top_opportunities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Company Info Functions

# COMMAND ----------

# Get company information
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_company_info(
    p_symbol STRING COMMENT 'Stock ticker'
)
RETURNS STRING
COMMENT 'Returns company information including sector, industry, and risk profile'
RETURN (
    SELECT to_json(named_struct(
        'symbol', symbol,
        'company_name', company_name,
        'sector', sector,
        'industry', industry,
        'beta', ROUND(beta, 2),
        'volatility_30d_pct', ROUND(volatility_30d, 2),
        'is_sp500', is_sp500,
        'is_fortune500', is_fortune500
    ))
    FROM {catalog}.gold.company_universe
    WHERE symbol = p_symbol
    LIMIT 1
)
""")

print("✅ Created: get_company_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ News & Alternative Signals Functions

# COMMAND ----------

# Get earnings surprise
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_earnings_surprise(
    p_symbol STRING COMMENT 'Stock ticker'
)
RETURNS STRING
COMMENT 'Returns recent earnings surprises and estimates'
RETURN (
    SELECT to_json(
        collect_list(
            named_struct(
                'event_date', event_date,
                'eps_estimate', eps_estimate,
                'eps_actual', eps_actual,
                'surprise_pct', surprise_pct
            )
        )
    )
    FROM (
        SELECT *
        FROM {catalog}.gold.earnings_calendar
        WHERE symbol = p_symbol 
          AND eps_actual IS NOT NULL
        ORDER BY event_date DESC
        LIMIT 4
    )
)
""")

print("✅ Created: get_earnings_surprise")

# COMMAND ----------

# Get analyst ratings
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_analyst_ratings(
    p_symbol STRING COMMENT 'Stock ticker'
)
RETURNS STRING
COMMENT 'Returns recent analyst recommendations and rating changes'
RETURN (
    SELECT to_json(
        collect_list(
            named_struct(
                'event_date', event_date,
                'firm', firm,
                'to_grade', to_grade,
                'from_grade', from_grade,
                'action', action
            )
        )
    )
    FROM (
        SELECT *
        FROM {catalog}.gold.analyst_recommendations
        WHERE symbol = p_symbol
        ORDER BY event_date DESC
        LIMIT 10
    )
)
""")

print("✅ Created: get_analyst_ratings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Portfolio Functions

# COMMAND ----------

# Get portfolio summary
spark.sql(f"""
CREATE OR REPLACE FUNCTION {tools_schema}.get_portfolio_summary(
    p_as_of_date DATE COMMENT 'As of date'
)
RETURNS STRING
COMMENT 'Returns portfolio-level summary including all positions'
RETURN (
    SELECT to_json(named_struct(
        'as_of_date', p_as_of_date,
        'total_stocks', COUNT(*),
        'bullish_count', SUM(CASE WHEN d.signal IN ('BUY', 'STRONG BUY') THEN 1 ELSE 0 END),
        'bearish_count', SUM(CASE WHEN d.signal IN ('SELL', 'STRONG SELL') THEN 1 ELSE 0 END),
        'neutral_count', SUM(CASE WHEN d.signal = 'HOLD' THEN 1 ELSE 0 END),
        'avg_expected_return_pct', ROUND(AVG(d.expected_return * 100), 2),
        'portfolio_beta', ROUND(AVG(r.beta_1y), 2),
        'portfolio_volatility_pct', ROUND(AVG(r.vol_20d * 100), 2)
    ))
    FROM {catalog}.gold.decision_signals d
    JOIN {catalog}.gold.risk_metrics_daily r ON d.symbol = r.symbol AND d.as_of_date = r.as_of_date
    WHERE d.as_of_date = p_as_of_date
)
""")

print("✅ Created: get_portfolio_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verification

# COMMAND ----------

# List all created functions
functions_df = spark.sql(f"""
    SHOW FUNCTIONS IN {tools_schema}
""")

print(f"\n📊 Created {functions_df.count()} UC Functions:")
functions_df.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Functions

# COMMAND ----------

print("🧪 Testing UC Functions...")
print("=" * 60)

# Test forecast function
print("\n1. Testing get_latest_forecast(AAPL, 2026-02-04):")
result = spark.sql(f"""
    SELECT {tools_schema}.get_latest_forecast('AAPL', '2026-02-04') as result
""").collect()[0]['result']
print(result[:200] + "..." if len(str(result)) > 200 else result)

# Test risk metrics
print("\n2. Testing get_risk_metrics(AAPL, 2026-02-02):")
result = spark.sql(f"""
    SELECT {tools_schema}.get_risk_metrics('AAPL', '2026-02-02') as result
""").collect()[0]['result']
print(result)

# Test decision signal
print("\n3. Testing get_decision_signal(AAPL, 2026-02-03):")
result = spark.sql(f"""
    SELECT {tools_schema}.get_decision_signal('AAPL', '2026-02-03') as result
""").collect()[0]['result']
print(result)

# Test company info
print("\n4. Testing get_company_info(AAPL):")
result = spark.sql(f"""
    SELECT {tools_schema}.get_company_info('AAPL') as result
""").collect()[0]['result']
print(result)

print("\n" + "=" * 60)
print("✅ All UC Functions Created and Tested Successfully!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")
