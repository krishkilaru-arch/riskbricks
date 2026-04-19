# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Create Gold Layer Analytics for RAG
# MAGIC
# MAGIC **Purpose**: Create aggregated, business-ready views from cleaned documents
# MAGIC
# MAGIC **Gold Tables Created**:
# MAGIC 1. `rag_document_summary` - Overall statistics
# MAGIC 2. `rag_stock_coverage` - Documents per stock
# MAGIC 3. `rag_sector_insights` - Sector-level aggregations
# MAGIC 4. `rag_filing_tracker` - SEC filing tracking
# MAGIC 5. `rag_news_timeline` - Recent news timeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

# Ensure functions schema exists for UC functions
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.functions")
print("✅ Ensured riskbricks.functions schema exists")

silver_table = f"{catalog}.silver.rag_documents"

print(f"📥 Source: {silver_table}")
print(f"📤 Target: {catalog}.gold.rag_*")

# Verify silver data exists
count = spark.sql(f"SELECT COUNT(*) FROM {silver_table}").collect()[0][0]
print(f"📊 Silver documents: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Gold Table 1: Document Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE riskbricks.gold.rag_document_summary AS
# MAGIC SELECT
# MAGIC   doc_type,
# MAGIC   COUNT(*) as total_documents,
# MAGIC   COUNT(DISTINCT symbol) as unique_stocks,
# MAGIC   COUNT(DISTINCT source) as unique_sources,
# MAGIC   ROUND(AVG(quality_score), 3) as avg_quality_score,
# MAGIC   ROUND(AVG(content_length), 0) as avg_content_length,
# MAGIC   MIN(published_date) as earliest_date,
# MAGIC   MAX(published_date) as latest_date,
# MAGIC   CURRENT_TIMESTAMP() as computed_at
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC GROUP BY doc_type
# MAGIC ORDER BY total_documents DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM riskbricks.gold.rag_document_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Gold Table 2: Stock Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE riskbricks.gold.rag_stock_coverage AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   COUNT(*) as total_documents,
# MAGIC   SUM(CASE WHEN doc_type = 'news' THEN 1 ELSE 0 END) as news_count,
# MAGIC   SUM(CASE WHEN doc_type = 'sec_10k' THEN 1 ELSE 0 END) as sec_10k_count,
# MAGIC   SUM(CASE WHEN doc_type = 'sec_10q' THEN 1 ELSE 0 END) as sec_10q_count,
# MAGIC   SUM(CASE WHEN doc_type = 'sec_8k' THEN 1 ELSE 0 END) as sec_8k_count,
# MAGIC   SUM(CASE WHEN doc_type = 'wiki_company' THEN 1 ELSE 0 END) as wiki_count,
# MAGIC   SUM(CASE WHEN doc_type = 'stock_context' THEN 1 ELSE 0 END) as stock_context_count,
# MAGIC   ROUND(AVG(quality_score), 3) as avg_quality,
# MAGIC   MAX(published_date) as latest_document_date,
# MAGIC   CURRENT_TIMESTAMP() as computed_at
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC GROUP BY symbol, company_name, sector
# MAGIC ORDER BY total_documents DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 20 stocks by coverage
# MAGIC SELECT 
# MAGIC   symbol, 
# MAGIC   company_name, 
# MAGIC   sector,
# MAGIC   total_documents,
# MAGIC   news_count,
# MAGIC   sec_10k_count + sec_10q_count + sec_8k_count as sec_filings,
# MAGIC   wiki_count
# MAGIC FROM riskbricks.gold.rag_stock_coverage
# MAGIC ORDER BY total_documents DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Gold Table 3: Sector Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE riskbricks.gold.rag_sector_insights AS
# MAGIC SELECT
# MAGIC   sector,
# MAGIC   COUNT(DISTINCT symbol) as unique_stocks,
# MAGIC   COUNT(*) as total_documents,
# MAGIC   SUM(CASE WHEN doc_type = 'news' THEN 1 ELSE 0 END) as news_count,
# MAGIC   SUM(CASE WHEN doc_type LIKE 'sec_%' THEN 1 ELSE 0 END) as sec_filing_count,
# MAGIC   ROUND(AVG(quality_score), 3) as avg_quality,
# MAGIC   ROUND(AVG(content_length), 0) as avg_content_length,
# MAGIC   MAX(published_date) as latest_document,
# MAGIC   CURRENT_TIMESTAMP() as computed_at
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC WHERE sector IS NOT NULL
# MAGIC GROUP BY sector
# MAGIC ORDER BY total_documents DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM riskbricks.gold.rag_sector_insights;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Gold Table 4: SEC Filing Tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE riskbricks.gold.rag_filing_tracker AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   doc_type,
# MAGIC   title,
# MAGIC   published_date as filing_date,
# MAGIC   source,
# MAGIC   url,
# MAGIC   quality_score,
# MAGIC   DATEDIFF(CURRENT_DATE(), published_date) as days_since_filing,
# MAGIC   CASE 
# MAGIC     WHEN doc_type = 'sec_10k' THEN 'Annual Report'
# MAGIC     WHEN doc_type = 'sec_10q' THEN 'Quarterly Report'
# MAGIC     WHEN doc_type = 'sec_8k' THEN 'Material Event'
# MAGIC     ELSE doc_type
# MAGIC   END as filing_type_label,
# MAGIC   CURRENT_TIMESTAMP() as computed_at
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC WHERE doc_type LIKE 'sec_%'
# MAGIC ORDER BY published_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent SEC filings
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   filing_type_label,
# MAGIC   filing_date,
# MAGIC   days_since_filing
# MAGIC FROM riskbricks.gold.rag_filing_tracker
# MAGIC ORDER BY filing_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Gold Table 5: News Timeline

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE riskbricks.gold.rag_news_timeline AS
# MAGIC SELECT
# MAGIC   published_date,
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   title,
# MAGIC   source,
# MAGIC   url,
# MAGIC   quality_score,
# MAGIC   CURRENT_TIMESTAMP() as computed_at
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC WHERE doc_type = 'news'
# MAGIC ORDER BY published_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent news by sector
# MAGIC SELECT 
# MAGIC   sector,
# MAGIC   COUNT(*) as news_count,
# MAGIC   MAX(published_date) as latest_news
# MAGIC FROM riskbricks.gold.rag_news_timeline
# MAGIC WHERE sector IS NOT NULL
# MAGIC GROUP BY sector
# MAGIC ORDER BY news_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Create UC Function for RAG Analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function to get stock document coverage
# MAGIC CREATE OR REPLACE FUNCTION riskbricks.functions.get_stock_rag_coverage(
# MAGIC     stock_symbol STRING COMMENT 'Stock ticker symbol'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   symbol STRING,
# MAGIC   company_name STRING,
# MAGIC   sector STRING,
# MAGIC   total_documents INT,
# MAGIC   news_count INT,
# MAGIC   sec_10k_count INT,
# MAGIC   sec_10q_count INT,
# MAGIC   sec_8k_count INT,
# MAGIC   wiki_count INT,
# MAGIC   latest_document_date STRING
# MAGIC )
# MAGIC COMMENT 'Get RAG document coverage for a specific stock'
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     symbol,
# MAGIC     company_name,
# MAGIC     sector,
# MAGIC     total_documents,
# MAGIC     news_count,
# MAGIC     sec_10k_count,
# MAGIC     sec_10q_count,
# MAGIC     sec_8k_count,
# MAGIC     wiki_count,
# MAGIC     latest_document_date
# MAGIC   FROM riskbricks.gold.rag_stock_coverage
# MAGIC   WHERE UPPER(symbol) = UPPER(stock_symbol);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function
# MAGIC SELECT * FROM riskbricks.functions.get_stock_rag_coverage('COST');

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold Layer Complete!

# COMMAND ----------

# Get counts for summary
doc_summary = spark.sql("SELECT COUNT(*) FROM riskbricks.gold.rag_document_summary").collect()[0][0]
stock_coverage = spark.sql("SELECT COUNT(*) FROM riskbricks.gold.rag_stock_coverage").collect()[0][0]
sector_insights = spark.sql("SELECT COUNT(*) FROM riskbricks.gold.rag_sector_insights").collect()[0][0]
filing_tracker = spark.sql("SELECT COUNT(*) FROM riskbricks.gold.rag_filing_tracker").collect()[0][0]
news_timeline = spark.sql("SELECT COUNT(*) FROM riskbricks.gold.rag_news_timeline").collect()[0][0]

print(f"""
================================================================================
✅ GOLD LAYER COMPLETE!
================================================================================

📊 Gold Tables Created:
   ┌─────────────────────────────────┬────────────────┬─────────────────────────┐
   │ Table                           │ Records        │ Purpose                 │
   ├─────────────────────────────────┼────────────────┼─────────────────────────┤
   │ rag_document_summary            │ {doc_summary:>8}       │ Overall statistics      │
   │ rag_stock_coverage              │ {stock_coverage:>8}       │ Per-stock coverage      │
   │ rag_sector_insights             │ {sector_insights:>8}       │ Sector aggregations     │
   │ rag_filing_tracker              │ {filing_tracker:>8}       │ SEC filing tracking     │
   │ rag_news_timeline               │ {news_timeline:>8}       │ News chronology         │
   └─────────────────────────────────┴────────────────┴─────────────────────────┘

🔧 UC Functions Created:
   - riskbricks.functions.get_stock_rag_coverage(symbol)

🎯 Use Cases:
   - "Which stocks have the most SEC filings?"
   - "Show me Costco's document coverage"
   - "What sectors have the most news?"
   - "Recent SEC 8-K material events?"

================================================================================
""")

dbutils.notebook.exit("success")
