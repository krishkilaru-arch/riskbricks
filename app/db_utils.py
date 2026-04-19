"""
Shared database connection utilities for all RiskBricks Streamlit pages.
Uses Databricks SDK unified auth (OAuth) — works in Databricks Apps.
"""

import os
import logging
import pandas as pd
import streamlit as st
from databricks.sdk.core import Config
from databricks import sql as dbsql

logger = logging.getLogger(__name__)

# Configurable catalog — defaults to "riskbricks", overridable via env var
CATALOG = os.getenv("RISKBRICKS_CATALOG", "riskbricks")


def _create_connection():
    """Create a new Databricks SQL connection using SDK OAuth credentials."""
    cfg = Config()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    host = (cfg.host or "").replace("https://", "")
    if not warehouse_id or not host:
        return None
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_resource
def _get_connection_holder():
    """Return a mutable dict to hold the connection (allows reconnection)."""
    return {"conn": _create_connection()}


def get_sql_connection():
    """Return a live Databricks SQL connection, reconnecting if needed."""
    holder = _get_connection_holder()
    conn = holder.get("conn")

    # Check if connection is alive; reconnect if stale or closed
    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("SQL connection stale — reconnecting.")
            try:
                conn.close()
            except Exception:
                pass
            conn = None

    if conn is None:
        conn = _create_connection()
        holder["conn"] = conn

    return conn


def run_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame. Handles reconnection."""
    conn = get_sql_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        # Retry once on connection error
        try:
            _get_connection_holder()["conn"] = None
            conn = get_sql_connection()
            if conn is not None:
                return pd.read_sql(query, conn)
        except Exception:
            pass
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def run_query_safe(query: str, params: dict = None) -> pd.DataFrame:
    """Execute a parameterized SQL query. Use %(name)s placeholders.

    Example:
        run_query_safe(
            "SELECT * FROM table WHERE name = %(name)s",
            {"name": "Sarah Russel"}
        )
    """
    conn = get_sql_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            cur.execute(query, parameters=params or {})
            if cur.description:
                cols = [d[0] for d in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=cols)
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def run_statement(statement: str, params: dict = None) -> bool:
    """Execute a DDL/DML statement with optional parameters. Returns True on success."""
    conn = get_sql_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(statement, parameters=params or {})
        return True
    except Exception as e:
        st.error(f"Statement error: {e}")
        return False
