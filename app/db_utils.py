"""
Shared database connection utilities for all RiskBricks Streamlit pages.
Uses Databricks SDK unified auth (OAuth) — works in Databricks Apps.
"""

import os
import pandas as pd
import streamlit as st
from databricks.sdk.core import Config
from databricks import sql as dbsql


@st.cache_resource
def get_sql_connection():
    """Return a Databricks SQL connection using SDK OAuth credentials."""
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


def run_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame."""
    conn = get_sql_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def run_statement(statement: str) -> bool:
    """Execute a DDL/DML statement. Returns True on success."""
    conn = get_sql_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
        return True
    except Exception as e:
        st.error(f"Statement error: {e}")
        return False
