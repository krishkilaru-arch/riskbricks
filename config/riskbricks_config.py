"""
RiskBricks - Central Configuration
===================================
Single source of truth for all workspace-specific settings.
Every notebook should import this instead of hardcoding paths.

Usage in notebooks:
    import sys
    sys.path.append("/Workspace" + _get_repo_root())
    from config.riskbricks_config import cfg
    
    # Then use:
    #   cfg.CATALOG, cfg.NOTEBOOKS_PATH, cfg.AGENTS_PATH, etc.
"""

import os


class RiskBricksConfig:
    """Auto-detecting workspace configuration."""

    # ── Catalog & Schemas ────────────────────────────────────────
    CATALOG = "riskbricks"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    TOOLS_SCHEMA = "agent_tools"
    FUNCTIONS_SCHEMA = "functions"

    # ── Secrets ──────────────────────────────────────────────────
    SECRETS_SCOPE = "riskbricks"
    FRED_API_KEY_SECRET = "fred-api-key"

    # ── Model Endpoints ──────────────────────────────────────────
    LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

    # ── Auto-detected paths (set at import time) ─────────────────
    REPO_ROOT = ""          # e.g. /Workspace/Users/user@company.com/vibe_coding/riskbricks
    NOTEBOOKS_PATH = ""     # REPO_ROOT/notebooks
    AGENTS_PATH = ""        # REPO_ROOT/notebooks/agents
    DATA_PATH = ""          # REPO_ROOT/data
    CONFIG_PATH = ""        # REPO_ROOT/config
    USER_EMAIL = ""         # e.g. user@company.com

    def __init__(self):
        self._detect_paths()

    def _detect_paths(self):
        """Auto-detect repo root from this file's location."""
        # This file lives at <repo_root>/config/riskbricks_config.py
        this_file = os.path.abspath(__file__)
        config_dir = os.path.dirname(this_file)
        repo_root = os.path.dirname(config_dir)

        self.REPO_ROOT = repo_root
        self.NOTEBOOKS_PATH = f"{repo_root}/notebooks"
        self.AGENTS_PATH = f"{repo_root}/notebooks/agents"
        self.DATA_PATH = f"{repo_root}/data"
        self.CONFIG_PATH = f"{repo_root}/config"

        # Extract user email from workspace path
        # Path format: /Workspace/Users/<email>/...
        parts = repo_root.split("/")
        try:
            users_idx = parts.index("Users")
            self.USER_EMAIL = parts[users_idx + 1]
        except (ValueError, IndexError):
            self.USER_EMAIL = ""

    # ── Convenience properties ───────────────────────────────────
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

    def agent_notebook(self, name):
        """Full path to an agent notebook for dbutils.notebook.run()."""
        return f"{self.AGENTS_PATH}/{name}"

    def notebook(self, relative_path):
        """Full path to any notebook by relative path from notebooks/."""
        return f"{self.NOTEBOOKS_PATH}/{relative_path}"

    def table(self, schema, name):
        """Fully qualified table name."""
        return f"{self.CATALOG}.{schema}.{name}"

    def __repr__(self):
        return (
            f"RiskBricksConfig(\n"
            f"  REPO_ROOT     = {self.REPO_ROOT}\n"
            f"  CATALOG       = {self.CATALOG}\n"
            f"  USER_EMAIL    = {self.USER_EMAIL}\n"
            f"  AGENTS_PATH   = {self.AGENTS_PATH}\n"
            f"  DATA_PATH     = {self.DATA_PATH}\n"
            f")"
        )


# Singleton instance — import this in notebooks
cfg = RiskBricksConfig()
