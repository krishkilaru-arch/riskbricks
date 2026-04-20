"""RiskBricks — Centralized Configuration Package

Single source of truth for every constant, symbol list, ML parameter,
and schema definition used across the project.

Usage (add to the top of any notebook)::

    import sys, os
    _nb  = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _root = "/Workspace" + (_nb[:_nb.find("/notebooks/")] if "/notebooks/" in _nb else os.path.dirname(_nb))
    sys.path.insert(0, _root)
    from config import *
"""

__all__ = [
    # Catalog & schemas
    "CATALOG", "SCHEMAS",
    # App / model / endpoint
    "APP_NAME", "ENDPOINT_NAME", "MODEL_NAME",
    # Dynamic loaders
    "get_symbols", "get_sector_map", "get_company_names", "sym_list_sql",
    # ML
    "CURATED_FEATURES", "LGB_PARAMS", "RF_PARAMS", "GB_PARAMS",
    # Data / jobs
    "FRED_SERIES", "KNOWN_MANAGERS", "JOB_SCHEDULES",
    # Fallbacks
    "FALLBACK_SYMBOLS", "FALLBACK_SECTOR_MAP", "FALLBACK_COMPANY_NAMES",
    "STRESS_SCENARIOS", "FRED_INDICATOR_META", "GDELT_COMPANY_KEYWORDS",
    # Logging
    "setup_logger", "log_step",
    # Class-based config singleton
    "cfg",
]

# Re-export static constants  (FALLBACK_* symbols, sector map, etc.)
from config.constants import (
    FALLBACK_SYMBOLS,
    FALLBACK_SECTOR_MAP,
    COMPANY_NAMES      as FALLBACK_COMPANY_NAMES,
    FRED_SERIES,
    KNOWN_MANAGERS,
    STRESS_SCENARIOS,
    FRED_INDICATOR_META,
    GDELT_COMPANY_KEYWORDS,
)

# Re-export the class-based config singleton
from config.riskbricks_config import cfg

import logging as _logging
import json as _json
from datetime import datetime as _datetime, timezone as _utc

_log = _logging.getLogger("riskbricks.config")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Catalog & Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Authoritative value — kept in sync with cfg.CATALOG
CATALOG = cfg.CATALOG
SCHEMAS = ["bronze", "silver", "gold", "agent_tools", "agents", "models", "pipelines", "monitoring"]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  App, Model & Endpoint Names
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
APP_NAME = "riskbricks"
ENDPOINT_NAME = cfg.AGENT_ENDPOINT          # single source: env var or default
MODEL_NAME = f"{CATALOG}.models.stock_forecast_ensemble"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Dynamic Symbol / Sector Helpers  (read from Unity Catalog)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_symbols(spark, catalog=CATALOG, fortune500_only=True):
    """Load all Fortune 500 symbols from gold.company_universe.

    Returns the full 430+ symbol list from the UC table.
    Falls back to FALLBACK_SYMBOLS (51 stocks) only during initial
    setup before Phase 1 populates the table.
    """
    try:
        where = "WHERE is_fortune500 = true" if fortune500_only else ""
        rows = spark.sql(
            f"SELECT DISTINCT symbol FROM {catalog}.gold.company_universe {where}"
        ).collect()
        symbols = sorted(r.symbol for r in rows)
        if symbols:
            return symbols
    except Exception as exc:
        _log.debug("get_symbols fell back to FALLBACK_SYMBOLS: %s", exc)
    return list(FALLBACK_SYMBOLS)


def get_sector_map(spark, catalog=CATALOG):
    """Load symbol → sector mapping from gold.company_universe."""
    try:
        rows = spark.sql(
            f"SELECT symbol, sector FROM {catalog}.gold.company_universe "
            "WHERE sector IS NOT NULL"
        ).collect()
        mapping = {r.symbol: r.sector for r in rows}
        if mapping:
            return mapping
    except Exception as exc:
        _log.debug("get_sector_map fell back to FALLBACK_SECTOR_MAP: %s", exc)
    return dict(FALLBACK_SECTOR_MAP)


def get_company_names(spark, catalog=CATALOG):
    """Load symbol → company_name mapping from gold.company_universe."""
    try:
        rows = spark.sql(
            f"SELECT symbol, company_name FROM {catalog}.gold.company_universe "
            "WHERE company_name IS NOT NULL"
        ).collect()
        mapping = {r.symbol: r.company_name for r in rows}
        if mapping:
            return mapping
    except Exception as exc:
        _log.debug("get_company_names fell back to FALLBACK_COMPANY_NAMES: %s", exc)
    return dict(FALLBACK_COMPANY_NAMES)


def sym_list_sql(symbols):
    """Convert a list of symbols to a SQL-safe IN-clause string.

    Strips any embedded quotes to prevent SQL injection.

    >>> sym_list_sql(["AAPL", "MSFT"])
    "'AAPL', 'MSFT'"
    """
    return ", ".join(f"'{s.replace(chr(39), '')}'" for s in symbols)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ML Feature List
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CURATED_FEATURES = [
    "return_5d",
    "return_20d",
    "volatility_20d",
    "ai_sentiment",
    "news_count",
    "gdelt_tone",
    "gdelt_events",
    "rsi_14",
    "macd_hist",
    "gap_pct",
    "sector_momentum_5d",
    "sector_breadth",
    "advance_ratio",
    "pct_above_ma20",
    "vix",
    "days_to_earnings",
    "is_monday",
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ML Hyper-parameters  (LightGBM + RandomForest + GradientBoosting)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LGB_PARAMS = dict(
    num_leaves=8,
    learning_rate=0.1,
    n_estimators=50,
    min_child_samples=3,
    random_state=42,
    verbose=-1,
    objective="binary",
)
RF_PARAMS = dict(
    n_estimators=100,
    max_depth=5,
    min_samples_leaf=3,
    random_state=42,
    n_jobs=-1,
)
GB_PARAMS = dict(
    n_estimators=50,
    max_depth=3,
    learning_rate=0.1,
    min_samples_leaf=3,
    random_state=42,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Job Schedules  (Quartz cron, America/New_York)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
JOB_SCHEDULES = {
    "daily_data_refresh":  "0 0 18 * * ?",    # 6:00 PM ET
    "news_to_forecasts":   "0 30 19 * * ?",   # 7:30 PM ET
    "ml_predictions":      "0 0 20 * * ?",    # 8:00 PM ET
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Logging Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def setup_logger(name):
    """Return a pre-configured logger for any RiskBricks notebook."""
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return _logging.getLogger(name)


def log_step(logger, step_name, table_name=None, row_count=None, error=None):
    """Emit a structured JSON log entry."""
    entry = {
        "step": step_name,
        "status": "ERROR" if error else "OK",
        "timestamp": _datetime.now(_utc.utc).isoformat(),
    }
    if table_name:
        entry["table"] = table_name
    if row_count is not None:
        entry["rows"] = row_count
    if error:
        entry["error"] = str(error)[:300]
    logger.info(_json.dumps(entry))
