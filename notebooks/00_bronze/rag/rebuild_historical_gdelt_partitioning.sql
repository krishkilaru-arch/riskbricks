-- Rebuild historical GDELT tables with partitioning by event_date + symbol
-- SAFE: creates new tables and keeps old until you confirm swap.

-- 1) Rebuild Events
CREATE TABLE IF NOT EXISTS riskbricks.bronze.historical_news_gdelt_rebuild
USING delta
PARTITIONED BY (event_date, symbol)
AS
SELECT * FROM riskbricks.bronze.historical_news_gdelt;

-- 2) Rebuild GKG
CREATE TABLE IF NOT EXISTS riskbricks.bronze.historical_news_gdelt_gkg_rebuild
USING delta
PARTITIONED BY (event_date, symbol)
AS
SELECT * FROM riskbricks.bronze.historical_news_gdelt_gkg;

-- 3) Validate row counts before swap
SELECT 'events_old' AS table_name, COUNT(*) AS cnt FROM riskbricks.bronze.historical_news_gdelt
UNION ALL
SELECT 'events_new', COUNT(*) FROM riskbricks.bronze.historical_news_gdelt_rebuild
UNION ALL
SELECT 'gkg_old', COUNT(*) FROM riskbricks.bronze.historical_news_gdelt_gkg
UNION ALL
SELECT 'gkg_new', COUNT(*) FROM riskbricks.bronze.historical_news_gdelt_gkg_rebuild;

-- 4) Swap (ONLY after validation)
-- ALTER TABLE riskbricks.bronze.historical_news_gdelt RENAME TO riskbricks.bronze.historical_news_gdelt_backup;
-- ALTER TABLE riskbricks.bronze.historical_news_gdelt_rebuild RENAME TO riskbricks.bronze.historical_news_gdelt;

-- ALTER TABLE riskbricks.bronze.historical_news_gdelt_gkg RENAME TO riskbricks.bronze.historical_news_gdelt_gkg_backup;
-- ALTER TABLE riskbricks.bronze.historical_news_gdelt_gkg_rebuild RENAME TO riskbricks.bronze.historical_news_gdelt_gkg;

-- 5) Drop backups ONLY after you confirm
-- DROP TABLE riskbricks.bronze.historical_news_gdelt_backup;
-- DROP TABLE riskbricks.bronze.historical_news_gdelt_gkg_backup;
