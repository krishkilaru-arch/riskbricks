# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 GDELT Data Analysis Demo
# MAGIC 
# MAGIC **Purpose**: Demonstrate what insights can be extracted from GDELT data
# MAGIC **without** knowing actual headlines
# MAGIC 
# MAGIC This notebook shows the VALUE and LIMITATIONS of GDELT data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query 1: Top Events by Impact (Articles Count)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the BIGGEST Costco stories by article count
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   actor1_name,
# MAGIC   actor2_name,
# MAGIC   avg_tone as sentiment,
# MAGIC   num_articles,
# MAGIC   CASE 
# MAGIC     WHEN avg_tone < -5 THEN '🔴 Very Negative'
# MAGIC     WHEN avg_tone < -2 THEN '🟠 Negative'
# MAGIC     WHEN avg_tone < 2 THEN '🟡 Neutral'
# MAGIC     WHEN avg_tone < 5 THEN '🟢 Positive'
# MAGIC     ELSE '🟢 Very Positive'
# MAGIC   END as sentiment_label
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE symbol = 'COST'
# MAGIC   AND num_articles >= 20
# MAGIC ORDER BY num_articles DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query 2: Government/Regulatory Events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find regulatory/government-related events
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   actor1_name,
# MAGIC   actor2_name,
# MAGIC   avg_tone as sentiment,
# MAGIC   num_articles,
# MAGIC   CASE 
# MAGIC     WHEN actor2_name IN ('ADMINISTRATION', 'GOVERNMENT', 'CONGRESS', 'FEDERAL COURT', 'SUPREME COURT', 'PRESIDENT') THEN 'Federal'
# MAGIC     WHEN actor2_name LIKE '%COURT%' THEN 'Legal'
# MAGIC     ELSE 'Other Government'
# MAGIC   END as govt_type
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE symbol = 'COST'
# MAGIC   AND (
# MAGIC     actor2_name IN ('ADMINISTRATION', 'GOVERNMENT', 'CONGRESS', 'FEDERAL COURT', 'SUPREME COURT', 'PRESIDENT')
# MAGIC     OR actor2_name LIKE '%COURT%'
# MAGIC     OR actor1_name IN ('ADMINISTRATION', 'GOVERNMENT', 'CONGRESS', 'FEDERAL COURT', 'SUPREME COURT')
# MAGIC   )
# MAGIC   AND num_articles >= 5
# MAGIC ORDER BY event_date DESC, num_articles DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query 3: International Expansion Timeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track Costco international mentions
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   COALESCE(
# MAGIC     CASE WHEN actor2_name IN ('CANADA', 'AUSTRALIA', 'NEW ZEALAND', 'MEXICO', 'CHINA', 'JAPAN', 'UNITED KINGDOM', 'ITALY', 'GERMANY') THEN actor2_name END,
# MAGIC     CASE WHEN actor1_name IN ('CANADA', 'AUSTRALIA', 'NEW ZEALAND', 'MEXICO', 'CHINA', 'JAPAN', 'UNITED KINGDOM', 'ITALY', 'GERMANY') THEN actor1_name END
# MAGIC   ) as country,
# MAGIC   avg_tone as sentiment,
# MAGIC   num_articles,
# MAGIC   actor1_name,
# MAGIC   actor2_name
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE symbol = 'COST'
# MAGIC   AND (
# MAGIC     actor2_name IN ('CANADA', 'AUSTRALIA', 'NEW ZEALAND', 'MEXICO', 'CHINA', 'JAPAN', 'UNITED KINGDOM', 'ITALY', 'GERMANY')
# MAGIC     OR actor1_name IN ('CANADA', 'AUSTRALIA', 'NEW ZEALAND', 'MEXICO', 'CHINA', 'JAPAN', 'UNITED KINGDOM', 'ITALY', 'GERMANY')
# MAGIC   )
# MAGIC   AND num_articles >= 5
# MAGIC ORDER BY event_date DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query 4: Sentiment Volatility by Month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly sentiment trend for Costco
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', event_date) as month,
# MAGIC   COUNT(*) as event_count,
# MAGIC   ROUND(AVG(avg_tone), 2) as avg_sentiment,
# MAGIC   ROUND(STDDEV(avg_tone), 2) as sentiment_volatility,
# MAGIC   SUM(num_articles) as total_articles,
# MAGIC   MIN(avg_tone) as worst_sentiment,
# MAGIC   MAX(avg_tone) as best_sentiment
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE symbol = 'COST'
# MAGIC GROUP BY DATE_TRUNC('month', event_date)
# MAGIC ORDER BY month DESC
# MAGIC LIMIT 24;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query 5: Crisis Detection (Large Negative Events)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detect potential crisis events
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   actor1_name,
# MAGIC   actor2_name,
# MAGIC   avg_tone as sentiment,
# MAGIC   num_articles,
# MAGIC   '⚠️ CRISIS SIGNAL' as alert
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE symbol = 'COST'
# MAGIC   AND avg_tone < -4  -- Very negative
# MAGIC   AND num_articles >= 10  -- Significant coverage
# MAGIC ORDER BY avg_tone ASC, num_articles DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query 6: Actor Network Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Who does Costco interact with most in news?
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN actor1_name = 'COSTCO' THEN actor2_name 
# MAGIC     ELSE actor1_name 
# MAGIC   END as other_actor,
# MAGIC   COUNT(*) as interaction_count,
# MAGIC   ROUND(AVG(avg_tone), 2) as avg_sentiment,
# MAGIC   SUM(num_articles) as total_articles,
# MAGIC   MIN(event_date) as first_interaction,
# MAGIC   MAX(event_date) as last_interaction
# MAGIC FROM riskbricks.bronze.historical_news_gdelt
# MAGIC WHERE symbol = 'COST'
# MAGIC   AND (actor1_name != '' OR actor2_name != '')
# MAGIC GROUP BY other_actor
# MAGIC HAVING COUNT(*) >= 5
# MAGIC ORDER BY interaction_count DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary: What GDELT Can and Cannot Tell You
# MAGIC 
# MAGIC ### ✅ CAN Determine:
# MAGIC - **Event timing**: When something significant happened
# MAGIC - **Event magnitude**: How big the story was (article count)
# MAGIC - **Sentiment direction**: Positive, negative, or neutral
# MAGIC - **Key actors**: Who was involved (government, countries, companies)
# MAGIC - **Patterns over time**: Trends, seasonality, crisis periods
# MAGIC 
# MAGIC ### ❌ CANNOT Determine:
# MAGIC - **What happened**: No headlines or article text
# MAGIC - **Why sentiment**: No context for positive/negative
# MAGIC - **Specific details**: No numbers, quotes, or facts
# MAGIC - **Source reliability**: URLs often null or unavailable
# MAGIC 
# MAGIC ### 🎯 For the RAG Agent:
# MAGIC To answer "What happened to Costco on September 26, 2024?", we need:
# MAGIC 1. **NewsAPI or SEC filings** for actual headlines
# MAGIC 2. **Vector Search** to find relevant articles
# MAGIC 3. **LLM synthesis** to summarize

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

