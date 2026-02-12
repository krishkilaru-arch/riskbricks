-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🧹 Clean Company Universe - Keep Top 20 Only
-- MAGIC 
-- MAGIC **Purpose**: Remove all stocks except the top 20 from company_universe
-- MAGIC 
-- MAGIC **Top 20 Stocks:**
-- MAGIC - AAPL, MSFT, GOOGL, AMZN, NVDA
-- MAGIC - META, TSLA, JPM, V, WMT
-- MAGIC - JNJ, PG, MA, HD, BAC
-- MAGIC - XOM, CVX, KO, DIS, NFLX

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Before Cleanup

-- COMMAND ----------

-- Current count
SELECT COUNT(*) as total_symbols
FROM riskbricks.gold.company_universe;

-- COMMAND ----------

-- Show all symbols (preview)
SELECT symbol, company_name, sector
FROM riskbricks.gold.company_universe
ORDER BY symbol
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 💾 Backup Original Table

-- COMMAND ----------

-- Create backup
CREATE OR REPLACE TABLE riskbricks.gold.company_universe_backup AS
SELECT * FROM riskbricks.gold.company_universe;

-- COMMAND ----------

-- Verify backup
SELECT COUNT(*) as backup_count
FROM riskbricks.gold.company_universe_backup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🗑️ Delete All Except Top 20

-- COMMAND ----------

-- Delete everything EXCEPT the top 20
DELETE FROM riskbricks.gold.company_universe
WHERE symbol NOT IN (
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'JPM', 'V', 'WMT',
    'JNJ', 'PG', 'MA', 'HD', 'BAC',
    'XOM', 'CVX', 'KO', 'DIS', 'NFLX'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Verify Top 20 Only

-- COMMAND ----------

-- Should return exactly 20
SELECT COUNT(*) as remaining_symbols
FROM riskbricks.gold.company_universe;

-- COMMAND ----------

-- Show all remaining symbols
SELECT 
    symbol,
    company_name,
    sector,
    industry,
    beta,
    volatility_30d,
    is_sp500,
    is_fortune500
FROM riskbricks.gold.company_universe
ORDER BY symbol;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Summary by Sector

-- COMMAND ----------

SELECT 
    sector,
    COUNT(*) as num_stocks,
    AVG(beta) as avg_beta,
    AVG(volatility_30d) as avg_volatility
FROM riskbricks.gold.company_universe
GROUP BY sector
ORDER BY num_stocks DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🔍 Verify Each Stock

-- COMMAND ----------

-- Check that we have all 20
SELECT 
    CASE 
        WHEN COUNT(*) = 20 THEN '✅ SUCCESS: Exactly 20 stocks'
        ELSE '❌ ERROR: Wrong count!'
    END as status,
    COUNT(*) as actual_count
FROM riskbricks.gold.company_universe;

-- COMMAND ----------

-- List all 20 with details
SELECT 
    ROW_NUMBER() OVER (ORDER BY symbol) as rank,
    symbol,
    company_name,
    sector,
    beta,
    volatility_30d
FROM riskbricks.gold.company_universe
ORDER BY symbol;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📋 Restore Instructions (If Needed)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **To restore original table:**
-- MAGIC ```sql
-- MAGIC DROP TABLE riskbricks.gold.company_universe;
-- MAGIC ALTER TABLE riskbricks.gold.company_universe_backup 
-- MAGIC   RENAME TO riskbricks.gold.company_universe;
-- MAGIC ```

-- COMMAND ----------

SELECT 
    'company_universe' as table_name,
    COUNT(*) as current_count
FROM riskbricks.gold.company_universe
UNION ALL
SELECT 
    'company_universe_backup' as table_name,
    COUNT(*) as backup_count
FROM riskbricks.gold.company_universe_backup;

-- COMMAND ----------

OPTIMIZE riskbricks.gold.company_universe;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Complete!
-- MAGIC 
-- MAGIC **company_universe now contains only 20 stocks:**
-- MAGIC 
-- MAGIC | Sector | Stocks |
-- MAGIC |--------|--------|
-- MAGIC | Technology | AAPL, MSFT, GOOGL, AMZN, NVDA, META |
-- MAGIC | Financials | JPM, V, MA, BAC |
-- MAGIC | Consumer Discretionary | TSLA, HD |
-- MAGIC | Consumer Staples | WMT, PG, KO |
-- MAGIC | Healthcare | JNJ |
-- MAGIC | Energy | XOM, CVX |
-- MAGIC | Communication Services | DIS, NFLX |

-- COMMAND ----------


