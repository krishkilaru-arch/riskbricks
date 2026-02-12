# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 Create News & Geopolitical Risk UC Functions
# MAGIC
# MAGIC **Purpose**: Create Unity Catalog functions for querying news and geopolitical risks
# MAGIC
# MAGIC **Functions Created:**
# MAGIC 1. `get_stock_news` - Get recent news for a stock
# MAGIC 2. `get_portfolio_news` - Get news affecting a portfolio manager's holdings
# MAGIC 3. `get_geopolitical_risks` - Get current geopolitical risk events
# MAGIC 4. `get_sector_sentiment` - Get sentiment summary for a sector
# MAGIC 5. `get_market_sentiment` - Get overall market sentiment from news

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG riskbricks;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ensure agent_tools schema exists
# MAGIC CREATE SCHEMA IF NOT EXISTS agent_tools;

# COMMAND ----------

print("✅ Schema ready: riskbricks.agent_tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📰 NEWS UC FUNCTIONS
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 13: get_stock_news()
# MAGIC
# MAGIC **Purpose**: Get recent news articles for a specific stock
# MAGIC **Parameters**: symbol (STRING), days_back (INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_stock_news(
# MAGIC   symbol STRING,
# MAGIC   days_back INT
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   title STRING,
# MAGIC   sentiment STRING,
# MAGIC   sentiment_score DOUBLE,
# MAGIC   risk_level STRING,
# MAGIC   portfolio_impact STRING,
# MAGIC   published_at TIMESTAMP,
# MAGIC   url STRING
# MAGIC )
# MAGIC COMMENT 'Returns recent news articles for a specific stock symbol with sentiment analysis.
# MAGIC Example: SELECT * FROM get_stock_news("AAPL", 7) for Apple news from last 7 days'
# MAGIC RETURN 
# MAGIC   SELECT 
# MAGIC     title,
# MAGIC     sentiment,
# MAGIC     sentiment_score,
# MAGIC     risk_level,
# MAGIC     portfolio_impact,
# MAGIC     published_at,
# MAGIC     url
# MAGIC   FROM riskbricks.silver.news_sentiment
# MAGIC   WHERE array_contains(all_symbols, get_stock_news.symbol)
# MAGIC     AND published_at >= CURRENT_DATE() - INTERVAL get_stock_news.days_back DAY
# MAGIC   ORDER BY published_at DESC;

# COMMAND ----------

print("✅ Function created: get_stock_news()")

# COMMAND ----------

# Test it
display(spark.sql("SELECT * FROM riskbricks.agent_tools.get_stock_news('AAPL', 7)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 14: get_portfolio_news()
# MAGIC
# MAGIC **Purpose**: Get news affecting a portfolio manager's holdings
# MAGIC **Parameters**: manager_name (STRING), days_back (INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_portfolio_news(
# MAGIC   manager_name STRING,
# MAGIC   days_back INT
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   title STRING,
# MAGIC   sentiment STRING,
# MAGIC   sentiment_score DOUBLE,
# MAGIC   risk_level STRING,
# MAGIC   affected_holdings ARRAY<STRING>,
# MAGIC   portfolio_impact STRING,
# MAGIC   published_at TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Returns news articles affecting a portfolio manager holdings.
# MAGIC Example: SELECT * FROM get_portfolio_news("Sarah Russel", 3) for Sarah news from last 3 days'
# MAGIC RETURN 
# MAGIC   SELECT 
# MAGIC     n.title,
# MAGIC     n.sentiment,
# MAGIC     n.sentiment_score,
# MAGIC     n.risk_level,
# MAGIC     array_intersect(n.all_symbols, collect_list(h.symbol)) as affected_holdings,
# MAGIC     n.portfolio_impact,
# MAGIC     n.published_at
# MAGIC   FROM riskbricks.silver.news_sentiment n
# MAGIC   CROSS JOIN (
# MAGIC     SELECT DISTINCT symbol 
# MAGIC     FROM riskbricks.gold.portfolio_holdings
# MAGIC     WHERE manager_name = get_portfolio_news.manager_name
# MAGIC   ) h
# MAGIC   WHERE EXISTS (
# MAGIC     SELECT 1 FROM unnest(n.all_symbols) as s
# MAGIC     WHERE s = h.symbol
# MAGIC   )
# MAGIC   AND n.published_at >= CURRENT_DATE() - INTERVAL get_portfolio_news.days_back DAY
# MAGIC   GROUP BY n.title, n.sentiment, n.sentiment_score, n.risk_level, n.all_symbols, n.portfolio_impact, n.published_at
# MAGIC   ORDER BY n.published_at DESC;

# COMMAND ----------

print("✅ Function created: get_portfolio_news()")

# COMMAND ----------

# Test it
display(spark.sql("SELECT * FROM riskbricks.agent_tools.get_portfolio_news('Sarah Russel', 7)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 15: get_geopolitical_risks()
# MAGIC
# MAGIC **Purpose**: Get current geopolitical risk events
# MAGIC **Parameters**: min_severity (INT) - minimum severity level (1-10)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_geopolitical_risks(
# MAGIC   min_severity INT
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   event_name STRING,
# MAGIC   event_category STRING,
# MAGIC   severity INT,
# MAGIC   description STRING,
# MAGIC   affected_sectors ARRAY<STRING>,
# MAGIC   estimated_impact_pct DOUBLE,
# MAGIC   event_date TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Returns current active geopolitical risk events.
# MAGIC Example: SELECT * FROM get_geopolitical_risks(6) for high-severity events (6+)'
# MAGIC RETURN 
# MAGIC   SELECT 
# MAGIC     event_name,
# MAGIC     event_category,
# MAGIC     severity,
# MAGIC     description,
# MAGIC     affected_sectors,
# MAGIC     estimated_market_impact_pct as estimated_impact_pct,
# MAGIC     event_date
# MAGIC   FROM riskbricks.gold.geopolitical_risk_events
# MAGIC   WHERE is_active = true
# MAGIC     AND severity >= get_geopolitical_risks.min_severity
# MAGIC   ORDER BY severity DESC, event_date DESC;

# COMMAND ----------

print("✅ Function created: get_geopolitical_risks()")

# COMMAND ----------

# Test it
display(spark.sql("SELECT * FROM riskbricks.agent_tools.get_geopolitical_risks(5)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 16: get_sector_sentiment()
# MAGIC
# MAGIC **Purpose**: Get sentiment summary for a sector
# MAGIC **Parameters**: sector_name (STRING), days_back (INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_sector_sentiment(
# MAGIC   sector_name STRING,
# MAGIC   days_back INT
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   sector STRING,
# MAGIC   avg_sentiment_score DOUBLE,
# MAGIC   bullish_articles INT,
# MAGIC   neutral_articles INT,
# MAGIC   bearish_articles INT,
# MAGIC   high_risk_articles INT,
# MAGIC   latest_article_date TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Returns sentiment summary for a specific sector.
# MAGIC Example: SELECT * FROM get_sector_sentiment("Technology", 7) for tech sector sentiment'
# MAGIC RETURN 
# MAGIC   SELECT 
# MAGIC     get_sector_sentiment.sector_name as sector,
# MAGIC     AVG(sentiment_score) as avg_sentiment_score,
# MAGIC     SUM(CASE WHEN sentiment = 'bullish' THEN 1 ELSE 0 END) as bullish_articles,
# MAGIC     SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_articles,
# MAGIC     SUM(CASE WHEN sentiment = 'bearish' THEN 1 ELSE 0 END) as bearish_articles,
# MAGIC     SUM(CASE WHEN risk_level = 'high' THEN 1 ELSE 0 END) as high_risk_articles,
# MAGIC     MAX(published_at) as latest_article_date
# MAGIC   FROM riskbricks.silver.news_sentiment
# MAGIC   WHERE array_contains(affected_sectors, get_sector_sentiment.sector_name)
# MAGIC     AND published_at >= CURRENT_DATE() - INTERVAL get_sector_sentiment.days_back DAY
# MAGIC   GROUP BY get_sector_sentiment.sector_name;

# COMMAND ----------

print("✅ Function created: get_sector_sentiment()")

# COMMAND ----------

# Test it
display(spark.sql("SELECT * FROM riskbricks.agent_tools.get_sector_sentiment('Technology', 7)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 17: get_market_sentiment()
# MAGIC
# MAGIC **Purpose**: Get overall market sentiment from all recent news
# MAGIC **Parameters**: days_back (INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.agent_tools.get_market_sentiment(
# MAGIC   days_back INT
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   metric STRING,
# MAGIC   value DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns overall market sentiment metrics.
# MAGIC Example: SELECT * FROM get_market_sentiment(1) for today market sentiment'
# MAGIC RETURN 
# MAGIC   SELECT metric, value FROM (
# MAGIC     SELECT 'avg_sentiment_score' as metric, AVG(sentiment_score) as value
# MAGIC     FROM riskbricks.silver.news_sentiment
# MAGIC     WHERE published_at >= CURRENT_DATE() - INTERVAL get_market_sentiment.days_back DAY
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 'bullish_pct', 
# MAGIC            100.0 * SUM(CASE WHEN sentiment = 'bullish' THEN 1 ELSE 0 END) / COUNT(*)
# MAGIC     FROM riskbricks.silver.news_sentiment
# MAGIC     WHERE published_at >= CURRENT_DATE() - INTERVAL get_market_sentiment.days_back DAY
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 'bearish_pct',
# MAGIC            100.0 * SUM(CASE WHEN sentiment = 'bearish' THEN 1 ELSE 0 END) / COUNT(*)
# MAGIC     FROM riskbricks.silver.news_sentiment
# MAGIC     WHERE published_at >= CURRENT_DATE() - INTERVAL get_market_sentiment.days_back DAY
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 'high_risk_pct',
# MAGIC            100.0 * SUM(CASE WHEN risk_level = 'high' THEN 1 ELSE 0 END) / COUNT(*)
# MAGIC     FROM riskbricks.silver.news_sentiment
# MAGIC     WHERE published_at >= CURRENT_DATE() - INTERVAL get_market_sentiment.days_back DAY
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 'total_articles',
# MAGIC            CAST(COUNT(*) AS DOUBLE)
# MAGIC     FROM riskbricks.silver.news_sentiment
# MAGIC     WHERE published_at >= CURRENT_DATE() - INTERVAL get_market_sentiment.days_back DAY
# MAGIC   );

# COMMAND ----------

print("✅ Function created: get_market_sentiment()")

# COMMAND ----------

# Test it
display(spark.sql("SELECT * FROM riskbricks.agent_tools.get_market_sentiment(1)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ALL NEWS UC FUNCTIONS CREATED!
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all UC functions
# MAGIC SHOW USER FUNCTIONS IN riskbricks.agent_tools;

# COMMAND ----------

print("""
================================================================================
✅ NEWS & GEOPOLITICAL RISK UC FUNCTIONS CREATED!
================================================================================

📰 NEW FUNCTIONS (13-17):
   13. get_stock_news(symbol, days_back)
   14. get_portfolio_news(manager_name, days_back)
   15. get_geopolitical_risks(min_severity)
   16. get_sector_sentiment(sector_name, days_back)
   17. get_market_sentiment(days_back)

📊 TOTAL UC FUNCTIONS: 17
   - Original 6: Portfolio management basics
   - Phase 2 (6): Advanced stock analytics
   - Phase 3 (5): News & geopolitical risk

📋 Example Queries:
   -- Get Apple news
   SELECT * FROM riskbricks.agent_tools.get_stock_news('AAPL', 7);
   
   -- Get Sarah's portfolio news
   SELECT * FROM riskbricks.agent_tools.get_portfolio_news('Sarah Russel', 3);
   
   -- Get high-severity geopolitical risks
   SELECT * FROM riskbricks.agent_tools.get_geopolitical_risks(7);
   
   -- Get tech sector sentiment
   SELECT * FROM riskbricks.agent_tools.get_sector_sentiment('Technology', 7);
   
   -- Get overall market sentiment
   SELECT * FROM riskbricks.agent_tools.get_market_sentiment(1);

🤖 Next: Integrate these into the AI agent for news-aware responses!

================================================================================
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

