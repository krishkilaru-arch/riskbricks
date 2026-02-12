# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 Unity Catalog Functions - Historical News Impact
# MAGIC
# MAGIC **Purpose**: Create UC functions for querying 12 years of news-price impact data
# MAGIC
# MAGIC **Functions**:
# MAGIC 1. `get_historical_news_impact(symbol, years_back)` - Stock-specific impact
# MAGIC 2. `get_sector_news_sensitivity(sector_name)` - Sector-level analysis
# MAGIC 3. `find_similar_events(sentiment_score, sector, days_back)` - Historical parallels
# MAGIC 4. `predict_portfolio_news_impact(manager_name, sentiment_score)` - Portfolio prediction
# MAGIC 5. `get_news_correlation_stats()` - Overall correlation metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup

# COMMAND ----------

USE CATALOG riskbricks;
USE SCHEMA agent_tools;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Function 1: Get Historical News Impact for a Stock

# COMMAND ----------

CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_historical_news_impact(
  symbol STRING COMMENT 'Stock ticker symbol (e.g., AAPL)',
  years_back INT COMMENT 'Number of years to look back (default 5)'
)
RETURNS TABLE(
  event_date DATE,
  sentiment_score DOUBLE,
  impact_1d_pct DOUBLE,
  impact_1w_pct DOUBLE,
  num_articles INT,
  event_description STRING,
  source_url STRING
)
COMMENT 'Returns historical news events and their actual price impact for a given stock'
RETURN 
  SELECT 
    event_date,
    sentiment_score,
    impact_1d_pct,
    impact_1w_pct,
    num_articles,
    actor1_name as event_description,
    source_url
  FROM riskbricks.gold.news_impact_history
  WHERE symbol = get_historical_news_impact.symbol
    AND event_date >= CURRENT_DATE() - INTERVAL get_historical_news_impact.years_back YEAR
    AND impact_1d_pct IS NOT NULL
  ORDER BY event_date DESC
  LIMIT 100;

-- Test
SELECT * FROM riskbricks.agent_tools.get_historical_news_impact('AAPL', 2);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Function 2: Sector News Sensitivity Analysis

# COMMAND ----------

CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_sector_news_sensitivity(
  sector_name STRING COMMENT 'Sector name (e.g., Technology)'
)
RETURNS TABLE(
  sector STRING,
  total_events INT,
  avg_abs_impact_1d DOUBLE,
  avg_impact_positive DOUBLE,
  avg_impact_negative DOUBLE,
  news_price_correlation DOUBLE,
  most_sensitive_stock STRING
)
COMMENT 'Returns how sensitive a sector is to news based on historical data'
RETURN 
  WITH sector_stats AS (
    SELECT 
      sector,
      COUNT(*) as total_events,
      AVG(ABS(impact_1d_pct)) as avg_abs_impact_1d,
      AVG(CASE WHEN sentiment_score > 0 THEN impact_1d_pct END) as avg_impact_positive,
      AVG(CASE WHEN sentiment_score < 0 THEN impact_1d_pct END) as avg_impact_negative,
      CORR(sentiment_score, impact_1d_pct) as news_price_correlation
    FROM riskbricks.gold.news_impact_history
    WHERE sector = get_sector_news_sensitivity.sector_name
    GROUP BY sector
  ),
  most_sensitive AS (
    SELECT 
      symbol,
      AVG(ABS(impact_1d_pct)) as avg_impact
    FROM riskbricks.gold.news_impact_history
    WHERE sector = get_sector_news_sensitivity.sector_name
    GROUP BY symbol
    ORDER BY avg_impact DESC
    LIMIT 1
  )
  SELECT 
    s.sector,
    s.total_events,
    ROUND(s.avg_abs_impact_1d, 2) as avg_abs_impact_1d,
    ROUND(s.avg_impact_positive, 2) as avg_impact_positive,
    ROUND(s.avg_impact_negative, 2) as avg_impact_negative,
    ROUND(s.news_price_correlation, 3) as news_price_correlation,
    m.symbol as most_sensitive_stock
  FROM sector_stats s
  CROSS JOIN most_sensitive m;

-- Test
SELECT * FROM riskbricks.agent_tools.get_sector_news_sensitivity('Technology');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Function 3: Find Similar Historical Events

# COMMAND ----------

CREATE OR REPLACE FUNCTION riskbricks.agent_tools.find_similar_events(
  sentiment_score DOUBLE COMMENT 'Target sentiment score (-10 to +10)',
  sector_name STRING COMMENT 'Sector to filter by',
  days_back INT COMMENT 'How many days back to search (default 1825 = 5 years)'
)
RETURNS TABLE(
  event_date DATE,
  symbol STRING,
  company_name STRING,
  sentiment_score DOUBLE,
  impact_1d_pct DOUBLE,
  impact_1w_pct DOUBLE,
  num_articles INT,
  event_description STRING
)
COMMENT 'Finds historical events with similar sentiment in a given sector'
RETURN 
  SELECT 
    event_date,
    symbol,
    company_name,
    sentiment_score,
    impact_1d_pct,
    impact_1w_pct,
    num_articles,
    actor1_name as event_description
  FROM riskbricks.gold.news_impact_history
  WHERE sector = find_similar_events.sector_name
    AND event_date >= CURRENT_DATE() - INTERVAL find_similar_events.days_back DAY
    AND ABS(sentiment_score - find_similar_events.sentiment_score) < 2.0
    AND impact_1d_pct IS NOT NULL
  ORDER BY num_articles DESC, ABS(impact_1d_pct) DESC
  LIMIT 20;

-- Test: Find bearish tech events similar to sentiment -5
SELECT * FROM riskbricks.agent_tools.find_similar_events(-5.0, 'Technology', 1825);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Function 4: Predict Portfolio Impact Based on Historical Data

# COMMAND ----------

CREATE OR REPLACE FUNCTION riskbricks.agent_tools.predict_portfolio_news_impact(
  manager_name STRING COMMENT 'Portfolio manager name',
  sentiment_score DOUBLE COMMENT 'News sentiment score (-10 to +10)'
)
RETURNS TABLE(
  manager_name STRING,
  predicted_impact_pct DOUBLE,
  confidence_level STRING,
  based_on_events INT,
  historical_range_low DOUBLE,
  historical_range_high DOUBLE,
  top_at_risk_holdings STRING
)
COMMENT 'Predicts portfolio impact based on historical news events with similar sentiment'
RETURN 
  WITH portfolio_holdings AS (
    SELECT 
      h.symbol,
      h.weight,
      c.sector
    FROM riskbricks.gold.portfolio_holdings h
    JOIN riskbricks.gold.portfolio_managers m ON h.manager_id = m.manager_id
    JOIN riskbricks.gold.company_universe c ON h.symbol = c.symbol
    WHERE m.manager_name = predict_portfolio_news_impact.manager_name
  ),
  historical_impacts AS (
    SELECT 
      n.symbol,
      n.sector,
      n.impact_1d_pct,
      n.num_articles
    FROM riskbricks.gold.news_impact_history n
    WHERE ABS(n.sentiment_score - predict_portfolio_news_impact.sentiment_score) < 2.0
      AND n.event_date >= CURRENT_DATE() - INTERVAL 3 YEAR
      AND n.impact_1d_pct IS NOT NULL
  ),
  sector_impacts AS (
    SELECT 
      sector,
      AVG(impact_1d_pct) as avg_sector_impact
    FROM historical_impacts
    GROUP BY sector
  ),
  portfolio_prediction AS (
    SELECT 
      SUM(p.weight * COALESCE(s.avg_sector_impact, 0)) as predicted_impact,
      COUNT(DISTINCT h.symbol) as num_events,
      MIN(s.avg_sector_impact) as range_low,
      MAX(s.avg_sector_impact) as range_high
    FROM portfolio_holdings p
    LEFT JOIN sector_impacts s ON p.sector = s.sector
    LEFT JOIN historical_impacts h ON p.symbol = h.symbol
  ),
  top_risks AS (
    SELECT 
      CONCAT_WS(', ', COLLECT_LIST(symbol)) as at_risk_symbols
    FROM (
      SELECT DISTINCT p.symbol
      FROM portfolio_holdings p
      JOIN sector_impacts s ON p.sector = s.sector
      WHERE s.avg_sector_impact < -2.0
      ORDER BY p.weight DESC
      LIMIT 3
    )
  )
  SELECT 
    predict_portfolio_news_impact.manager_name as manager_name,
    ROUND(p.predicted_impact, 2) as predicted_impact_pct,
    CASE 
      WHEN p.num_events >= 100 THEN 'High'
      WHEN p.num_events >= 30 THEN 'Medium'
      ELSE 'Low'
    END as confidence_level,
    p.num_events as based_on_events,
    ROUND(p.range_low, 2) as historical_range_low,
    ROUND(p.range_high, 2) as historical_range_high,
    r.at_risk_symbols as top_at_risk_holdings
  FROM portfolio_prediction p
  CROSS JOIN top_risks r;

-- Test
SELECT * FROM riskbricks.agent_tools.predict_portfolio_news_impact('Mohit Arora', -5.0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Function 5: Overall News-Price Correlation Statistics

# COMMAND ----------

CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_news_correlation_stats()
RETURNS TABLE(
  total_events_analyzed BIGINT,
  date_range_start DATE,
  date_range_end DATE,
  correlation_1day DOUBLE,
  correlation_1week DOUBLE,
  correlation_1month DOUBLE,
  avg_impact_very_negative DOUBLE,
  avg_impact_negative DOUBLE,
  avg_impact_neutral DOUBLE,
  avg_impact_positive DOUBLE,
  avg_impact_very_positive DOUBLE
)
COMMENT 'Returns overall statistics showing how news sentiment correlates with price movements'
RETURN 
  WITH overall_stats AS (
    SELECT 
      COUNT(*) as total_events,
      MIN(event_date) as date_start,
      MAX(event_date) as date_end,
      CORR(sentiment_score, impact_1d_pct) as corr_1d,
      CORR(sentiment_score, impact_1w_pct) as corr_1w,
      CORR(sentiment_score, impact_1m_pct) as corr_1m
    FROM riskbricks.gold.news_impact_history
    WHERE impact_1d_pct IS NOT NULL
  ),
  sentiment_buckets AS (
    SELECT 
      AVG(CASE WHEN sentiment_score < -5 THEN impact_1d_pct END) as very_neg,
      AVG(CASE WHEN sentiment_score >= -5 AND sentiment_score < -2 THEN impact_1d_pct END) as neg,
      AVG(CASE WHEN sentiment_score >= -2 AND sentiment_score < 2 THEN impact_1d_pct END) as neutral,
      AVG(CASE WHEN sentiment_score >= 2 AND sentiment_score < 5 THEN impact_1d_pct END) as pos,
      AVG(CASE WHEN sentiment_score >= 5 THEN impact_1d_pct END) as very_pos
    FROM riskbricks.gold.news_impact_history
    WHERE impact_1d_pct IS NOT NULL
  )
  SELECT 
    o.total_events as total_events_analyzed,
    o.date_start as date_range_start,
    o.date_end as date_range_end,
    ROUND(o.corr_1d, 3) as correlation_1day,
    ROUND(o.corr_1w, 3) as correlation_1week,
    ROUND(o.corr_1m, 3) as correlation_1month,
    ROUND(b.very_neg, 2) as avg_impact_very_negative,
    ROUND(b.neg, 2) as avg_impact_negative,
    ROUND(b.neutral, 2) as avg_impact_neutral,
    ROUND(b.pos, 2) as avg_impact_positive,
    ROUND(b.very_pos, 2) as avg_impact_very_positive
  FROM overall_stats o
  CROSS JOIN sentiment_buckets b;

-- Test
SELECT * FROM riskbricks.agent_tools.get_news_correlation_stats();

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ All Functions Created!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all historical news UC functions
# MAGIC SHOW USER FUNCTIONS IN riskbricks.agent_tools 
# MAGIC LIKE '*news*';

# COMMAND ----------

print("""
================================================================================
✅ HISTORICAL NEWS UC FUNCTIONS CREATED!
================================================================================

📊 5 New Functions Available:

1. get_historical_news_impact(symbol, years_back)
   → "Show me how AAPL historically reacted to news"
   
2. get_sector_news_sensitivity(sector_name)
   → "Which sectors are most sensitive to news?"
   
3. find_similar_events(sentiment_score, sector, days_back)
   → "Find similar bearish tech events from the past"
   
4. predict_portfolio_news_impact(manager_name, sentiment_score)
   → "How would Mohit's portfolio react to negative news?"
   
5. get_news_correlation_stats()
   → "Does news actually move prices? Show me the data"

🎯 These functions enable the AI agent to:
   - Answer: "Based on 12 years of data..."
   - Provide: "In 847 similar events, we saw..."
   - Predict: "Historically, this type of news causes X% impact"

📋 Next Step:
   Update AI agent to use these functions for richer, data-driven responses

================================================================================
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

