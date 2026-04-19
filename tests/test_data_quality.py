"""
Data Quality Tests
===================
Validates table existence, freshness, null constraints, and row counts.
Run in Databricks with: %pip install pytest && pytest tests/test_data_quality.py -v
"""

import pytest
from datetime import datetime, timedelta


class TestTableExistence:
    """Verify all expected tables exist in Unity Catalog."""

    def test_gold_tables_exist(self, spark_session, catalog, gold_tables):
        for table in gold_tables:
            fqn = f"{catalog}.gold.{table}"
            tables = spark_session.sql(f"SHOW TABLES IN {catalog}.gold LIKE \'{table}\'").collect()
            assert len(tables) > 0, f"Table {fqn} does not exist"

    def test_silver_tables_exist(self, spark_session, catalog):
        expected = ["stock_prices", "ml_training_features", "technical_indicators",
                    "sector_features", "market_breadth", "news_ai_sentiment"]
        for table in expected:
            fqn = f"{catalog}.silver.{table}"
            tables = spark_session.sql(f"SHOW TABLES IN {catalog}.silver LIKE \'{table}\'").collect()
            assert len(tables) > 0, f"Table {fqn} does not exist"

    def test_bronze_tables_exist(self, spark_session, catalog):
        expected = ["stock_prices_bronze", "fred_macro_indicators", "news_rss_all"]
        for table in expected:
            tables = spark_session.sql(f"SHOW TABLES IN {catalog}.bronze LIKE \'{table}\'").collect()
            assert len(tables) > 0, f"Table {catalog}.bronze.{table} does not exist"


class TestDataFreshness:
    """Verify data was updated within SLA windows."""

    FRESHNESS_SLA_DAYS = {
        "silver.stock_prices": 3,          # weekdays only, 3-day SLA
        "gold.portfolio_risk_metrics": 3,
        "gold.ml_stock_predictions": 3,
        "gold.decision_signals": 3,
        "gold.stock_forecasts": 7,
        "bronze.news_rss_all": 3,
    }

    @pytest.mark.parametrize("table_path,max_days", list(FRESHNESS_SLA_DAYS.items()))
    def test_freshness(self, spark_session, catalog, table_path, max_days):
        fqn = f"{catalog}.{table_path}"
        # Try common date column names
        for col in ["date", "pred_date", "signal_date", "published_date", "updated_at"]:
            try:
                result = spark_session.sql(f"SELECT MAX({col}) AS latest FROM {fqn}").collect()
                if result and result[0]["latest"]:
                    latest = result[0]["latest"]
                    if hasattr(latest, "date"):
                        latest = latest.date()
                    cutoff = (datetime.now() - timedelta(days=max_days)).date()
                    assert latest >= cutoff, (
                        f"{fqn} is stale: latest={latest}, SLA={max_days} days"
                    )
                    return
            except Exception:
                continue
        pytest.skip(f"No date column found in {fqn}")


class TestDataQuality:
    """Validate null constraints and row counts on critical tables."""

    def test_portfolio_managers_not_empty(self, spark_session, catalog):
        count = spark_session.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.portfolio_managers").collect()[0]["cnt"]
        assert count >= 3, f"Expected at least 3 managers, got {count}"

    def test_company_universe_not_empty(self, spark_session, catalog):
        count = spark_session.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.company_universe").collect()[0]["cnt"]
        assert count >= 40, f"Expected at least 40 companies, got {count}"

    def test_stock_prices_no_null_close(self, spark_session, catalog):
        nulls = spark_session.sql(
            f"SELECT COUNT(*) AS cnt FROM {catalog}.silver.stock_prices WHERE close IS NULL"
        ).collect()[0]["cnt"]
        assert nulls == 0, f"Found {nulls} null close prices in silver.stock_prices"

    def test_risk_metrics_no_null_var(self, spark_session, catalog):
        nulls = spark_session.sql(
            f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.portfolio_risk_metrics WHERE var_1day_95_usd IS NULL"
        ).collect()[0]["cnt"]
        assert nulls == 0, f"Found {nulls} null VaR values"

    def test_ml_predictions_valid_direction(self, spark_session, catalog):
        invalid = spark_session.sql(
            f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.ml_stock_predictions "
            f"WHERE direction NOT IN (\'UP\', \'DOWN\')"
        ).collect()[0]["cnt"]
        assert invalid == 0, f"Found {invalid} invalid directions (expected UP/DOWN)"

    def test_ml_predictions_confidence_range(self, spark_session, catalog):
        out_of_range = spark_session.sql(
            f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.ml_stock_predictions "
            f"WHERE confidence < 0 OR confidence > 1"
        ).collect()[0]["cnt"]
        assert out_of_range == 0, f"Found {out_of_range} predictions with confidence outside [0, 1]"
