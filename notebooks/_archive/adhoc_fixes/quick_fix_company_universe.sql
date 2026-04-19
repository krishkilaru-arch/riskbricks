-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🔧 Quick Fix: Clean Company Universe
-- MAGIC
-- MAGIC Removes invalid symbols (delisted, ETFs) from company_universe

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Before: Current State

-- COMMAND ----------

SELECT COUNT(*) as total_symbols
FROM riskbricks.gold.company_universe;

-- COMMAND ----------

-- Show some of the problematic symbols
SELECT symbol, company_name, sector
FROM riskbricks.gold.company_universe
WHERE symbol IN ('MRO', 'SPY', 'SLV', 'PKI', 'PXD', 'RAD', 'HES', 'JNPR', 'ANSS')
ORDER BY symbol;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Backup Original Table

-- COMMAND ----------

CREATE OR REPLACE TABLE riskbricks.gold.company_universe_backup AS
SELECT * FROM riskbricks.gold.company_universe;

-- COMMAND ----------

SELECT COUNT(*) as backup_count 
FROM riskbricks.gold.company_universe_backup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Remove Invalid Symbols

-- COMMAND ----------

-- Delete invalid symbols
DELETE FROM riskbricks.gold.company_universe
WHERE symbol IN (
    -- Definitely delisted or acquired (verified 2026)
    'CHX',   -- Delisted
    'HES',   -- Hess (acquired by Chevron in 2024)
    'JNPR',  -- Juniper (acquired by HPE in 2024)
    'PKI',   -- PerkinElmer (split into two companies 2023)
    'PXD',   -- Pioneer Natural (acquired by Exxon 2024)
    'RAD',   -- Rite Aid (bankrupt/delisted)
    
    -- ETFs (not companies, no earnings)
    'SPY',   -- S&P 500 ETF
    'SLV',   -- Silver ETF
    'GLD',   -- Gold ETF
    'QQQ',   -- Nasdaq 100 ETF
    'IWM',   -- Russell 2000 ETF
    'EEM',   -- Emerging Markets ETF
    'TLT',   -- Treasury ETF
    'HYG'    -- High Yield ETF
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## After: Verify Fix

-- COMMAND ----------

SELECT COUNT(*) as remaining_symbols
FROM riskbricks.gold.company_universe;

-- COMMAND ----------

-- Check that invalid symbols are gone
SELECT symbol, company_name
FROM riskbricks.gold.company_universe
WHERE symbol IN ('SPY', 'SLV', 'PKI', 'PXD', 'RAD', 'HES', 'JNPR')
ORDER BY symbol;

-- Should return 0 rows

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary

-- COMMAND ----------

SELECT 
    (SELECT COUNT(*) FROM riskbricks.gold.company_universe_backup) as original_count,
    (SELECT COUNT(*) FROM riskbricks.gold.company_universe) as cleaned_count,
    (SELECT COUNT(*) FROM riskbricks.gold.company_universe_backup) - 
    (SELECT COUNT(*) FROM riskbricks.gold.company_universe) as removed_count;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Done!
-- MAGIC
-- MAGIC Invalid symbols removed. You can now re-run:
-- MAGIC - `ingest_alt_signals_yfinance` (fewer errors)
-- MAGIC - All agent notebooks will use clean symbol list

-- COMMAND ----------


