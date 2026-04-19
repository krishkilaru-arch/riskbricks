"""
RiskBricks Test Configuration
==============================
Shared fixtures for all test modules.
Run with: pytest tests/ -v --tb=short
"""

import pytest
import os

CATALOG = os.getenv("RISKBRICKS_CATALOG", "riskbricks")


@pytest.fixture
def catalog():
    return CATALOG


@pytest.fixture
def spark_session():
    """Return active SparkSession (only works in Databricks runtime)."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception:
        pytest.skip("SparkSession not available outside Databricks")


@pytest.fixture
def gold_tables():
    """List of expected gold-layer tables."""
    return [
        "company_universe",
        "portfolio_holdings",
        "portfolio_managers",
        "portfolio_risk_metrics",
        "stress_test_results",
        "stock_forecasts",
        "decision_signals",
        "ml_stock_predictions",
    ]


@pytest.fixture
def uc_functions():
    """List of expected UC agent tool functions."""
    return [
        "get_portfolio_risk_metrics",
        "get_stress_test_results",
        "get_stock_forecast",
        "get_ml_stock_forecast",
        "get_ml_market_overview",
        "get_factor_exposures",
        "get_sector_exposures",
        "get_decision_signal",
        "get_macro_context",
        "get_news_context",
        "get_portfolio_holdings",
    ]
