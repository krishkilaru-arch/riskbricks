"""
Data Management — Monitor freshness, trigger jobs, review pipeline health
"""

import streamlit as st
import pandas as pd
import os, sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db_utils import run_query

st.set_page_config(page_title="Data Management", page_icon="⚙️", layout="wide")
st.title("⚙️ Data Management")
st.caption("Monitor data freshness, trigger refresh jobs, and review pipeline health.")

# ── Data freshness ───────────────────────────────────────────────────────────
st.markdown("### 📈 Data Freshness")

stock_fresh = run_query("""
    SELECT MAX(date) AS latest_date, COUNT(DISTINCT symbol) AS symbols, COUNT(*) AS records
    FROM riskbricks.bronze.stock_prices_bronze
""")
macro_fresh = run_query("""
    SELECT MAX(date) AS latest_date, COUNT(DISTINCT indicator_name) AS indicators, COUNT(*) AS records
    FROM riskbricks.bronze.macro_indicators_bronze
""")
company_cnt = run_query("SELECT COUNT(*) AS cnt FROM riskbricks.gold.company_universe")
forecast_cnt = run_query("SELECT COUNT(*) AS cnt FROM riskbricks.gold.stock_forecasts")

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
        c1.success("✅ Current")
    elif days_old is not None:
        c1.warning(f"⚠️ {days_old} days old")
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
    c3.metric("Company Universe", "—")

if not forecast_cnt.empty:
    c4.metric("Stock Forecasts", f"{int(forecast_cnt.iloc[0]['cnt']):,} predictions")
else:
    c4.metric("Stock Forecasts", "—")

st.markdown("---")

# ── Layer summary ────────────────────────────────────────────────────────────
st.markdown("### 🏗️ Medallion Layer Health")

layers = {
    "Bronze": [
        ("stock_prices_bronze", "riskbricks.bronze.stock_prices_bronze"),
        ("macro_indicators_bronze", "riskbricks.bronze.macro_indicators_bronze"),
        ("portfolio_holdings_bronze", "riskbricks.bronze.portfolio_holdings_bronze"),
    ],
    "Silver": [
        ("stock_prices", "riskbricks.silver.stock_prices"),
        ("macro_indicators", "riskbricks.silver.macro_indicators"),
        ("forecast_features_daily", "riskbricks.silver.forecast_features_daily"),
    ],
    "Gold": [
        ("portfolio_risk_metrics", "riskbricks.gold.portfolio_risk_metrics"),
        ("stress_test_results", "riskbricks.gold.stress_test_results"),
        ("stock_forecasts", "riskbricks.gold.stock_forecasts"),
        ("decision_signals", "riskbricks.gold.decision_signals"),
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
            rows.append({"Table": label, "Rows": f"{cnt:,}", "Status": "✅" if cnt > 0 else "⚠️ Empty"})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.markdown("---")

# ── Trigger jobs ─────────────────────────────────────────────────────────────
st.markdown("### 🔄 Trigger Data Refresh")
st.info("Scheduled workflows are not yet configured. Use the notebooks in `00_bronze/`, `02_silver/`, and `03_gold/` to refresh data manually, or set up a Databricks Workflow for automated daily runs.")

if st.button("🔄 Clear cached data"):
    st.cache_data.clear()
    st.rerun()
