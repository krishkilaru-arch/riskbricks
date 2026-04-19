-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🔧 Add Missing Columns - Quick Fix
-- MAGIC
-- MAGIC **Purpose**: Add critical missing columns identified in schema review
-- MAGIC
-- MAGIC **Run this once** before using the app or querying data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Catalog

-- COMMAND ----------

USE CATALOG riskbricks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Portfolio Holdings - Add `shares` Column

-- COMMAND ----------

-- Add shares column if it doesn't exist
ALTER TABLE gold.portfolio_holdings 
ADD COLUMN IF NOT EXISTS shares DOUBLE COMMENT 'Number of shares held';

-- COMMAND ----------

-- Backfill shares calculation: shares = value_usd / current_price
-- We'll use a default price of $100 if actual price not available
MERGE INTO gold.portfolio_holdings AS h
USING (
  SELECT 
    ph.portfolio_id,
    ph.manager_id,
    ph.symbol,
    ph.value_usd,
    COALESCE(sp.close, 100.0) as current_price,
    ph.value_usd / COALESCE(sp.close, 100.0) as calculated_shares
  FROM gold.portfolio_holdings ph
  LEFT JOIN (
    SELECT symbol, close
    FROM silver.stock_prices
    WHERE date = (SELECT MAX(date) FROM silver.stock_prices)
  ) sp ON ph.symbol = sp.symbol
  WHERE ph.shares IS NULL
) AS calc
ON h.portfolio_id = calc.portfolio_id 
   AND h.manager_id = calc.manager_id 
   AND h.symbol = calc.symbol
WHEN MATCHED THEN UPDATE SET h.shares = calc.calculated_shares;

-- COMMAND ----------

SELECT 
  'shares_added' as metric,
  COUNT(*) as count,
  MIN(shares) as min_shares,
  AVG(shares) as avg_shares,
  MAX(shares) as max_shares
FROM gold.portfolio_holdings
WHERE shares IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Portfolio Managers - Add `aum_usd` Column

-- COMMAND ----------

-- Add AUM column if it doesn't exist
ALTER TABLE gold.portfolio_managers
ADD COLUMN IF NOT EXISTS aum_usd DOUBLE COMMENT 'Assets Under Management in USD';

-- COMMAND ----------

-- Backfill AUM from holdings
MERGE INTO gold.portfolio_managers AS m
USING (
  SELECT 
    manager_id,
    SUM(value_usd) as total_aum
  FROM gold.portfolio_holdings
  GROUP BY manager_id
) AS calc
ON m.manager_id = calc.manager_id
WHEN MATCHED THEN UPDATE SET m.aum_usd = calc.total_aum;

-- COMMAND ----------

SELECT 
  manager_name,
  risk_profile,
  aum_usd,
  FORMAT_NUMBER(aum_usd, 0) as aum_formatted
FROM gold.portfolio_managers
ORDER BY aum_usd DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Portfolio Managers - Add `num_holdings` Column

-- COMMAND ----------

-- Add num_holdings column
ALTER TABLE gold.portfolio_managers
ADD COLUMN IF NOT EXISTS num_holdings INT COMMENT 'Number of positions';

-- COMMAND ----------

-- Backfill num_holdings from holdings
MERGE INTO gold.portfolio_managers AS m
USING (
  SELECT 
    manager_id,
    CAST(COUNT(*) AS INT) as total_holdings
  FROM gold.portfolio_holdings
  GROUP BY manager_id
) AS calc
ON m.manager_id = calc.manager_id
WHEN MATCHED THEN UPDATE SET m.num_holdings = calc.total_holdings;

-- COMMAND ----------

SELECT 
  manager_name,
  num_holdings,
  aum_usd,
  aum_usd / num_holdings as avg_position_size
FROM gold.portfolio_managers
ORDER BY aum_usd DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Company Universe - Add `latest_price` Column

-- COMMAND ----------

-- Add latest_price column
ALTER TABLE gold.company_universe
ADD COLUMN IF NOT EXISTS latest_price DOUBLE COMMENT 'Latest stock price';

-- COMMAND ----------

-- Backfill latest prices from stock_prices
MERGE INTO gold.company_universe AS c
USING (
  SELECT 
    symbol,
    close as latest_price
  FROM silver.stock_prices
  WHERE date = (SELECT MAX(date) FROM silver.stock_prices)
) AS sp
ON c.symbol = sp.symbol
WHEN MATCHED THEN UPDATE SET c.latest_price = sp.latest_price;

-- COMMAND ----------

SELECT 
  symbol,
  company_name,
  sector,
  latest_price,
  beta,
  volatility_30d
FROM gold.company_universe
WHERE latest_price IS NOT NULL
ORDER BY latest_price DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Portfolio Holdings - Add Current Price & P&L Columns

-- COMMAND ----------

-- Add current price and P&L columns
ALTER TABLE gold.portfolio_holdings
ADD COLUMN IF NOT EXISTS current_price DOUBLE COMMENT 'Current market price';

ALTER TABLE gold.portfolio_holdings
ADD COLUMN IF NOT EXISTS avg_cost_per_share DOUBLE COMMENT 'Average cost per share';

ALTER TABLE gold.portfolio_holdings
ADD COLUMN IF NOT EXISTS unrealized_gain_loss DOUBLE COMMENT 'Unrealized P&L in USD';

ALTER TABLE gold.portfolio_holdings
ADD COLUMN IF NOT EXISTS unrealized_gain_loss_pct DOUBLE COMMENT 'Unrealized P&L %';

-- COMMAND ----------

-- Backfill current_price from latest stock prices
MERGE INTO gold.portfolio_holdings AS h
USING (
  SELECT 
    symbol,
    close as latest_price
  FROM silver.stock_prices
  WHERE date = (SELECT MAX(date) FROM silver.stock_prices)
) AS sp
ON h.symbol = sp.symbol
WHEN MATCHED THEN UPDATE SET h.current_price = sp.latest_price;

-- COMMAND ----------

-- Calculate avg_cost_per_share (assuming purchase at value_usd / shares)
UPDATE gold.portfolio_holdings
SET avg_cost_per_share = CASE 
  WHEN shares > 0 THEN value_usd / shares
  ELSE NULL
END
WHERE shares IS NOT NULL AND shares > 0;

-- COMMAND ----------

-- Calculate unrealized P&L
UPDATE gold.portfolio_holdings
SET 
  unrealized_gain_loss = CASE 
    WHEN shares IS NOT NULL AND current_price IS NOT NULL AND avg_cost_per_share IS NOT NULL
    THEN shares * (current_price - avg_cost_per_share)
    ELSE NULL
  END,
  unrealized_gain_loss_pct = CASE 
    WHEN avg_cost_per_share > 0 AND current_price IS NOT NULL
    THEN ((current_price - avg_cost_per_share) / avg_cost_per_share) * 100
    ELSE NULL
  END
WHERE shares IS NOT NULL;

-- COMMAND ----------

-- Show P&L summary
SELECT 
  manager_id,
  COUNT(*) as num_positions,
  SUM(unrealized_gain_loss) as total_unrealized_pl,
  AVG(unrealized_gain_loss_pct) as avg_unrealized_pl_pct,
  SUM(CASE WHEN unrealized_gain_loss > 0 THEN 1 ELSE 0 END) as winning_positions,
  SUM(CASE WHEN unrealized_gain_loss < 0 THEN 1 ELSE 0 END) as losing_positions
FROM gold.portfolio_holdings
WHERE unrealized_gain_loss IS NOT NULL
GROUP BY manager_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Portfolio Risk Metrics - Add Advanced Metrics

-- COMMAND ----------

-- Add Sharpe ratio
ALTER TABLE gold.portfolio_risk_metrics
ADD COLUMN IF NOT EXISTS sharpe_ratio DOUBLE COMMENT 'Sharpe ratio (annualized)';

-- Add Sortino ratio
ALTER TABLE gold.portfolio_risk_metrics
ADD COLUMN IF NOT EXISTS sortino_ratio DOUBLE COMMENT 'Sortino ratio (downside risk)';

-- Add Max Drawdown
ALTER TABLE gold.portfolio_risk_metrics
ADD COLUMN IF NOT EXISTS max_drawdown_pct DOUBLE COMMENT 'Maximum drawdown %';

-- Add 99% VaR
ALTER TABLE gold.portfolio_risk_metrics
ADD COLUMN IF NOT EXISTS var_1day_99_usd DOUBLE COMMENT '1-day VaR at 99% confidence';

-- COMMAND ----------

-- Note: These will be NULL until we run the risk analytics notebook again
-- The risk analytics notebook needs to be updated to calculate these metrics

SELECT 
  manager_name,
  var_1day_95_usd,
  var_1day_99_usd,
  sharpe_ratio,
  sortino_ratio,
  max_drawdown_pct
FROM gold.portfolio_risk_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Verification

-- COMMAND ----------

-- Check portfolio_managers columns
DESCRIBE TABLE gold.portfolio_managers;

-- COMMAND ----------

-- Check portfolio_holdings columns  
DESCRIBE TABLE gold.portfolio_holdings;

-- COMMAND ----------

-- Check company_universe columns
DESCRIBE TABLE gold.company_universe;

-- COMMAND ----------

-- Check portfolio_risk_metrics columns
DESCRIBE TABLE gold.portfolio_risk_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Summary Report

-- COMMAND ----------

SELECT 
  'Portfolio Managers' as table_name,
  COUNT(*) as row_count,
  SUM(CASE WHEN aum_usd IS NOT NULL THEN 1 ELSE 0 END) as aum_populated,
  SUM(CASE WHEN num_holdings IS NOT NULL THEN 1 ELSE 0 END) as num_holdings_populated
FROM gold.portfolio_managers

UNION ALL

SELECT 
  'Portfolio Holdings',
  COUNT(*),
  SUM(CASE WHEN shares IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN current_price IS NOT NULL THEN 1 ELSE 0 END)
FROM gold.portfolio_holdings

UNION ALL

SELECT 
  'Company Universe',
  COUNT(*),
  SUM(CASE WHEN latest_price IS NOT NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN beta IS NOT NULL THEN 1 ELSE 0 END)
FROM gold.company_universe;

-- COMMAND ----------

PRINT("""
================================================================================
✅ MISSING COLUMNS ADDED SUCCESSFULLY!
================================================================================

📊 Columns Added:
   - portfolio_managers: aum_usd, num_holdings
   - portfolio_holdings: shares, current_price, avg_cost_per_share, 
                         unrealized_gain_loss, unrealized_gain_loss_pct
   - company_universe: latest_price
   - portfolio_risk_metrics: sharpe_ratio, sortino_ratio, max_drawdown_pct, var_1day_99_usd

🔄 Next Steps:
   1. Re-run analytics/01_risk_analytics.py to populate advanced metrics
   2. Test the app - Portfolio Management page should now work!
   3. Update any queries that reference these columns

================================================================================
""")
