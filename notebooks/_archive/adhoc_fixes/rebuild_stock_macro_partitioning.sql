-- Rebuild stock + macro bronze tables with partitioning (safe, no data loss)
-- Creates new tables and keeps old ones until you confirm swap.

-- 1) Rebuild stock prices
CREATE TABLE IF NOT EXISTS riskbricks.bronze.stock_prices_bronze_rebuild
USING delta
PARTITIONED BY (date, symbol)
AS
SELECT * FROM riskbricks.bronze.stock_prices_bronze;

-- 2) Rebuild macro indicators
CREATE TABLE IF NOT EXISTS riskbricks.bronze.macro_indicators_bronze_rebuild
USING delta
PARTITIONED BY (date, indicator_name)
AS
SELECT * FROM riskbricks.bronze.macro_indicators_bronze;

-- 3) Validate row counts before swap
SELECT 'stock_old' AS table_name, COUNT(*) AS cnt FROM riskbricks.bronze.stock_prices_bronze
UNION ALL
SELECT 'stock_new', COUNT(*) FROM riskbricks.bronze.stock_prices_bronze_rebuild
UNION ALL
SELECT 'macro_old', COUNT(*) FROM riskbricks.bronze.macro_indicators_bronze
UNION ALL
SELECT 'macro_new', COUNT(*) FROM riskbricks.bronze.macro_indicators_bronze_rebuild;

-- 4) Swap (ONLY after validation)
-- ALTER TABLE riskbricks.bronze.stock_prices_bronze RENAME TO riskbricks.bronze.stock_prices_bronze_backup;
-- ALTER TABLE riskbricks.bronze.stock_prices_bronze_rebuild RENAME TO riskbricks.bronze.stock_prices_bronze;

-- ALTER TABLE riskbricks.bronze.macro_indicators_bronze RENAME TO riskbricks.bronze.macro_indicators_bronze_backup;
-- ALTER TABLE riskbricks.bronze.macro_indicators_bronze_rebuild RENAME TO riskbricks.bronze.macro_indicators_bronze;

-- 5) Drop backups ONLY after you confirm
-- DROP TABLE riskbricks.bronze.stock_prices_bronze_backup;
-- DROP TABLE riskbricks.bronze.macro_indicators_bronze_backup;
