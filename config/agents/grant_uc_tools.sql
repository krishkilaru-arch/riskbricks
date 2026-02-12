-- Replace <principal> with your user, group, or service principal
GRANT USAGE ON CATALOG riskbricks TO `<principal>`;
GRANT USAGE ON SCHEMA riskbricks.tools TO `<principal>`;

GRANT EXECUTE ON FUNCTION riskbricks.tools.get_latest_forecast TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_risk_metrics TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_decision_signal TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_news_context TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_news_impact_stats TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_factor_exposures TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_forecast_eval TO `<principal>`;
GRANT EXECUTE ON FUNCTION riskbricks.tools.get_geopolitical_events TO `<principal>`;

-- Optional: grant read access on gold tables if needed for ad hoc inspection
-- GRANT SELECT ON SCHEMA riskbricks.gold TO `<principal>`;
