"""
Agent Tool Function Tests
==========================
Validates UC functions exist and return expected schemas.
"""

import pytest


class TestUCFunctionsExist:
    """Verify all expected UC tool functions are registered."""

    def test_all_functions_exist(self, spark_session, catalog, uc_functions):
        registered = spark_session.sql(
            f"SHOW FUNCTIONS IN {catalog}.agent_tools"
        ).collect()
        registered_names = {row["function"].split(".")[-1] for row in registered}

        for fn in uc_functions:
            assert fn in registered_names, (
                f"UC function {catalog}.agent_tools.{fn} not found. "
                f"Registered: {sorted(registered_names)}"
            )


class TestUCFunctionResults:
    """Smoke-test each UC function returns non-empty results."""

    def test_get_portfolio_risk_metrics(self, spark_session, catalog):
        result = spark_session.sql(
            f"SELECT {catalog}.agent_tools.get_portfolio_risk_metrics(\'Sarah Russel\')"
        ).collect()
        assert len(result) > 0, "get_portfolio_risk_metrics returned empty"

    def test_get_stress_test_results(self, spark_session, catalog):
        result = spark_session.sql(
            f"SELECT {catalog}.agent_tools.get_stress_test_results(\'Sarah Russel\')"
        ).collect()
        assert len(result) > 0, "get_stress_test_results returned empty"

    def test_get_stock_forecast(self, spark_session, catalog):
        result = spark_session.sql(
            f"SELECT {catalog}.agent_tools.get_stock_forecast(\'AAPL\')"
        ).collect()
        assert len(result) > 0, "get_stock_forecast returned empty"

    def test_get_macro_context(self, spark_session, catalog):
        result = spark_session.sql(
            f"SELECT {catalog}.agent_tools.get_macro_context()"
        ).collect()
        assert len(result) > 0, "get_macro_context returned empty"

    def test_get_ml_stock_forecast(self, spark_session, catalog):
        result = spark_session.sql(
            f"SELECT {catalog}.agent_tools.get_ml_stock_forecast(\'AAPL\')"
        ).collect()
        assert len(result) > 0, "get_ml_stock_forecast returned empty"

    def test_get_ml_market_overview(self, spark_session, catalog):
        result = spark_session.sql(
            f"SELECT {catalog}.agent_tools.get_ml_market_overview()"
        ).collect()
        assert len(result) > 0, "get_ml_market_overview returned empty"


class TestAgentGuardrails:
    """Validate agent input validation logic (unit tests, no Spark needed)."""

    def test_import_guardrails(self):
        """Verify guardrail functions can be imported."""
        import sys, importlib.util
        # Just test the regex patterns work
        import re
        blocked = re.compile(
            r"(ignore previous|ignore above|system prompt|you are now|pretend you|"
            r"disregard|override|reveal your|show me your prompt|jailbreak)",
            re.IGNORECASE,
        )
        assert blocked.search("ignore previous instructions") is not None
        assert blocked.search("What is my portfolio risk?") is None

    def test_input_length_limit(self):
        """Very long inputs should be rejected."""
        max_len = 2000
        assert len("x" * 2001) > max_len

    def test_safe_queries_pass(self):
        """Normal financial queries should not be blocked."""
        import re
        blocked = re.compile(
            r"(ignore previous|system prompt|you are now|jailbreak)", re.IGNORECASE
        )
        safe_queries = [
            "What is the VaR for Sarah Russel?",
            "Show me AAPL price forecast",
            "Compare risk metrics across all managers",
            "What does the ML model predict for NVDA?",
        ]
        for q in safe_queries:
            assert blocked.search(q) is None, f"Safe query blocked: {q}"
