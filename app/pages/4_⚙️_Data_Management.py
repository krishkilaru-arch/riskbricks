"""
Data Management — Monitor freshness, trigger jobs, review pipeline health
"""

import streamlit as st
import pandas as pd
import os, sys
from datetime import datetime

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from db_utils import run_query, CATALOG

st.set_page_config(page_title="Data Management", page_icon="\u2699\ufe0f", layout="wide")
st.title("\u2699\ufe0f Data Management")
st.caption("Monitor data freshness, trigger refresh jobs, and review pipeline health.")

# ── Data freshness ───────────────────────────────────────────────────────────
st.markdown("### \U0001f4c8 Data Freshness")

stock_fresh = run_query(f"""
    SELECT MAX(date) AS latest_date, COUNT(DISTINCT symbol) AS symbols, COUNT(*) AS records
    FROM {CATALOG}.bronze.stock_prices_bronze
""")
macro_fresh = run_query(f"""
    SELECT MAX(date) AS latest_date, COUNT(DISTINCT indicator) AS indicators, COUNT(*) AS records
    FROM {CATALOG}.bronze.fred_macro_indicators
""")
company_cnt = run_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.company_universe")
forecast_cnt = run_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.stock_forecasts")

c1, c2, c3, c4 = st.columns(4)

if not stock_fresh.empty and stock_fresh.iloc[0]["latest_date"] is not None:
    sd = stock_fresh.iloc[0]
    latest = sd["latest_date"]
    try:
        days_old = (datetime.now().date() - pd.Timestamp(latest).date()).days
    except Exception:
        days_old = None
    c1.metric("Stock Prices", f"{int(sd['records']):,} rows",
              delta=f"as of {latest}" if latest else None)
    if days_old is not None and days_old <= 1:
        c1.success("\u2705 Current")
    elif days_old is not None:
        c1.warning(f"\u26a0\ufe0f {days_old} days old")
else:
    c1.metric("Stock Prices", "No data")

if not macro_fresh.empty and macro_fresh.iloc[0]["latest_date"] is not None:
    md = macro_fresh.iloc[0]
    c2.metric("Macro Indicators", f"{int(md['records']):,} rows",
              delta=f"{int(md['indicators'])} indicators")
else:
    c2.metric("Macro Indicators", "No data")

if not company_cnt.empty:
    c3.metric("Company Universe", f"{int(company_cnt.iloc[0]['cnt']):,} stocks")
else:
    c3.metric("Company Universe", "\u2014")

if not forecast_cnt.empty:
    c4.metric("Stock Forecasts", f"{int(forecast_cnt.iloc[0]['cnt']):,} predictions")
else:
    c4.metric("Stock Forecasts", "\u2014")

st.markdown("---")

# ── Layer summary ────────────────────────────────────────────────────────────
st.markdown("### \U0001f3d7\ufe0f Medallion Layer Health")

layers = {
    "Bronze": [
        ("stock_prices_bronze", f"{CATALOG}.bronze.stock_prices_bronze"),
        ("fred_macro_indicators", f"{CATALOG}.bronze.fred_macro_indicators"),
        ("news_rss_all", f"{CATALOG}.bronze.news_rss_all"),
        ("historical_news_gdelt", f"{CATALOG}.bronze.historical_news_gdelt"),
        ("portfolio_holdings_bronze", f"{CATALOG}.bronze.portfolio_holdings_bronze"),
    ],
    "Silver": [
        ("stock_prices", f"{CATALOG}.silver.stock_prices"),
        ("technical_indicators", f"{CATALOG}.silver.technical_indicators"),
        ("sector_features", f"{CATALOG}.silver.sector_features"),
        ("market_breadth", f"{CATALOG}.silver.market_breadth"),
        ("forecast_features_daily", f"{CATALOG}.silver.forecast_features_daily"),
        ("ml_training_features", f"{CATALOG}.silver.ml_training_features"),
    ],
    "Gold": [
        ("portfolio_risk_metrics", f"{CATALOG}.gold.portfolio_risk_metrics"),
        ("stress_test_results", f"{CATALOG}.gold.stress_test_results"),
        ("stock_forecasts", f"{CATALOG}.gold.stock_forecasts"),
        ("decision_signals", f"{CATALOG}.gold.decision_signals"),
        ("ml_stock_predictions", f"{CATALOG}.gold.ml_stock_predictions"),
        ("company_universe", f"{CATALOG}.gold.company_universe"),
        ("portfolio_holdings", f"{CATALOG}.gold.portfolio_holdings"),
    ],
}

for layer_name, tables in layers.items():
    with st.expander(f"**{layer_name} Layer** ({len(tables)} key tables)", expanded=False):
        rows = []
        for label, fqn in tables:
            try:
                cnt_df = run_query(f"SELECT COUNT(*) AS cnt FROM {fqn}")
                cnt = int(cnt_df.iloc[0]["cnt"]) if not cnt_df.empty else 0
            except Exception:
                cnt = 0
            rows.append({"Table": label, "Rows": f"{cnt:,}", "Status": "\u2705" if cnt > 0 else "\u26a0\ufe0f Empty"})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.markdown("---")

# ── Trigger jobs ─────────────────────────────────────────────────────────────
st.markdown("### \U0001f504 Trigger Data Refresh")
st.info("Use the Databricks Workflows page to trigger scheduled jobs, or run individual notebooks from the project.")

if st.button("\U0001f504 Clear cached data"):
    st.cache_data.clear()
    st.rerun()
