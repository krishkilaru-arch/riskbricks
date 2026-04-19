"""
RiskBricks - Central Configuration
===================================
Single source of truth for all workspace-specific settings.

Usage in notebooks:
    import sys
    sys.path.append("/Workspace" + _get_repo_root())
    from config.riskbricks_config import cfg

Usage in Streamlit app:
    from config.riskbricks_config import cfg
"""

import os


class RiskBricksConfig:
    """Auto-detecting workspace configuration."""

    # ── Catalog & Schemas ────────────────────────────────────────
    CATALOG = os.getenv("RISKBRICKS_CATALOG", "riskbricks")
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    TOOLS_SCHEMA = "agent_tools"
    AGENTS_SCHEMA = "agents"
    MODELS_SCHEMA = "models"

    # ── Secrets ──────────────────────────────────────────────────
    SECRETS_SCOPE = "riskbricks"
    FRED_API_KEY_SECRET = "fred-api-key"

    # ── Model Endpoints ──────────────────────────────────────────
    LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
    AGENT_ENDPOINT = os.getenv("RISKBRICKS_AGENT_ENDPOINT", "riskbricks-supervisor-agent")

    # ── Auto-detected paths (set at import time) ─────────────────
    REPO_ROOT = ""
    NOTEBOOKS_PATH = ""
    AGENTS_PATH = ""
    DATA_PATH = ""
    CONFIG_PATH = ""
    USER_EMAIL = ""

    def __init__(self):
        self._detect_paths()

    def _detect_paths(self):
        """Auto-detect repo root from this file's location."""
        this_file = os.path.abspath(__file__)
        config_dir = os.path.dirname(this_file)
        repo_root = os.path.dirname(config_dir)

        self.REPO_ROOT = repo_root
        self.NOTEBOOKS_PATH = f"{repo_root}/notebooks"
        self.AGENTS_PATH = f"{repo_root}/notebooks/agents"
        self.DATA_PATH = f"{repo_root}/data"
        self.CONFIG_PATH = f"{repo_root}/config"

        parts = repo_root.split("/")
        try:
            users_idx = parts.index("Users")
            self.USER_EMAIL = parts[users_idx + 1]
        except (ValueError, IndexError):
            self.USER_EMAIL = ""

    # ── Fully-qualified table names ──────────────────────────────
    def table(self, schema: str, name: str) -> str:
        return f"{self.CATALOG}.{schema}.{name}"

    @property
    def bronze_db(self):
        return f"{self.CATALOG}.{self.BRONZE_SCHEMA}"

    @property
    def silver_db(self):
        return f"{self.CATALOG}.{self.SILVER_SCHEMA}"

    @property
    def gold_db(self):
        return f"{self.CATALOG}.{self.GOLD_SCHEMA}"

    @property
    def tools_db(self):
        return f"{self.CATALOG}.{self.TOOLS_SCHEMA}"

    @property
    def agents_db(self):
        return f"{self.CATALOG}.{self.AGENTS_SCHEMA}"

    @property
    def models_db(self):
        return f"{self.CATALOG}.{self.MODELS_SCHEMA}"

    def agent_notebook(self, name):
        return f"{self.AGENTS_PATH}/{name}"

    def notebook(self, relative_path):
        return f"{self.NOTEBOOKS_PATH}/{relative_path}"

    def __repr__(self):
        return (
            f"RiskBricksConfig(\n"
            f"  CATALOG       = {self.CATALOG}\n"
            f"  REPO_ROOT     = {self.REPO_ROOT}\n"
            f"  USER_EMAIL    = {self.USER_EMAIL}\n"
            f"  AGENT_ENDPOINT= {self.AGENT_ENDPOINT}\n"
            f")"
        )


# Singleton instance
cfg = RiskBricksConfig()
