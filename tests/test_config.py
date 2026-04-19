"""
Configuration Tests
====================
Validates project configuration consistency.
"""

import os
import pytest


class TestProjectStructure:
    """Verify expected project files and directories exist."""

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @pytest.mark.parametrize("path", [
        "databricks.yml",
        "requirements.txt",
        "README.md",
        "DATA_ARCHITECTURE.md",
        "app/Home.py",
        "app/db_utils.py",
        "notebooks/agents/riskbricks_agent.py",
        "notebooks/jobs/daily_data_refresh.ipynb",
        "notebooks/jobs/daily_gdelt_refresh.ipynb",
        "notebooks/training/train_register_ensemble_model.ipynb",
    ])
    def test_file_exists(self, path):
        full = os.path.join(self.ROOT, path)
        assert os.path.exists(full), f"Expected file not found: {path}"

    @pytest.mark.parametrize("directory", [
        "notebooks/agents",
        "notebooks/jobs",
        "notebooks/ingestion",
        "notebooks/gold",
        "notebooks/training",
        "app/pages",
        "config",
        "tests",
    ])
    def test_directory_exists(self, directory):
        full = os.path.join(self.ROOT, directory)
        assert os.path.isdir(full), f"Expected directory not found: {directory}"


class TestAgentConfig:
    """Validate agent configuration is consistent."""

    def test_agent_file_has_guardrails(self):
        agent_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "notebooks", "agents", "riskbricks_agent.py"
        )
        with open(agent_path) as f:
            content = f.read()
        assert "validate_input" in content, "Agent missing input validation"
        assert "sanitize_output" in content, "Agent missing output sanitization"
        assert "MAX_GRAPH_RECURSION" in content, "Agent missing recursion limit"
        assert "audit" in content.lower(), "Agent missing audit logging"

    def test_requirements_has_key_packages(self):
        req_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "requirements.txt"
        )
        with open(req_path) as f:
            content = f.read()
        for pkg in ["lightgbm", "langchain", "langgraph", "mlflow", "databricks-sdk"]:
            assert pkg in content, f"requirements.txt missing {pkg}"
