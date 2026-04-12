"""
RiskBricks — AI-Powered Portfolio Risk Analytics
"""

import streamlit as st
import pandas as pd
import os, sys

APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from db_utils import run_query

st.set_page_config(page_title="RiskBricks", page_icon="📊", layout="wide")

st.markdown("""
<style>
    .hero {text-align:center; padding:1.5rem 0 0.5rem;}
    .hero h1 {font-size:2.6rem; font-weight:800; margin:0; color:#1B2A4A;}
    .hero p  {font-size:1.15rem; color:#6c757d; margin-top:0.25rem;}
    div[data-testid="stMetric"] {
        background:#f8f9fc; border-radius:12px; padding:18px 16px;
        border-left:4px solid #3B82F6;
    }
    .section-hdr {font-size:1.25rem; font-weight:700; color:#1B2A4A; margin:1.8rem 0 0.6rem;}
    .feat-card {background:#ffffff; border:1px solid #e5e7eb; border-radius:12px;
                 padding:1.2rem; min-height:180px; height:100%; display:flex;
                 flex-direction:column; justify-content:flex-start;}
    .feat-icon {font-size:2rem; margin-bottom:0.4rem;}
    .feat-title {font-weight:700; font-size:1rem; margin:0.3rem 0;}
    .feat-desc  {font-size:0.88rem; color:#6c757d; line-height:1.5;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="hero">
    <h1>📊 RiskBricks</h1>
    <p>AI-Powered Portfolio Risk Analytics Platform</p>
</div>
""", unsafe_allow_html=True)

# ── Live metrics ─────────────────────────────────────────────────────────────
managers_df = run_query("""
    SELECT manager_name, risk_profile, aum_usd
    FROM riskbricks.gold.portfolio_managers
""")
holdings_cnt = run_query("SELECT COUNT(*) AS cnt FROM riskbricks.gold.portfolio_holdings")
universe_cnt = run_query("SELECT COUNT(*) AS cnt FROM riskbricks.gold.company_universe")
freshness    = run_query("SELECT MAX(date) AS latest FROM riskbricks.silver.stock_prices")
risk_df      = run_query("""
    SELECT manager_name, var_1day_95_usd, portfolio_beta,
           weighted_volatility_pct, aum_usd
    FROM riskbricks.gold.portfolio_risk_metrics
""")

has_data = not managers_df.empty

if has_data:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total AUM",     f"${managers_df['aum_usd'].sum()/1e6:,.1f}M")
    c2.metric("Managers",       len(managers_df))
    c3.metric("Positions",      f"{int(holdings_cnt.iloc[0]['cnt']):,}" if not holdings_cnt.empty else "—")
    c4.metric("Stock Universe", f"{int(universe_cnt.iloc[0]['cnt']):,}" if not universe_cnt.empty else "—")
    c5.metric("Data As-Of",     str(freshness.iloc[0]["latest"]) if not freshness.empty else "—")
else:
    st.info("Connecting to data… If this persists, check the SQL warehouse resource in App settings.")

# ── Risk snapshot ────────────────────────────────────────────────────────────
if has_data and not risk_df.empty:
    st.markdown('<p class="section-hdr">Risk Snapshot — Manager Comparison</p>', unsafe_allow_html=True)
    disp = risk_df.copy()
    disp.columns = ["Manager", "1-Day VaR (95%)", "Beta", "Volatility %", "AUM"]
    disp["1-Day VaR (95%)"] = disp["1-Day VaR (95%)"].apply(lambda v: f"${v:,.0f}")
    disp["Beta"]            = disp["Beta"].apply(lambda v: f"{v:.2f}")
    disp["Volatility %"]    = disp["Volatility %"].apply(lambda v: f"{v:.1f}%")
    disp["AUM"]             = disp["AUM"].apply(lambda v: f"${v/1e6:,.1f}M")
    st.dataframe(disp, use_container_width=True, hide_index=True)

# ── Feature cards ────────────────────────────────────────────────────────────
st.markdown('<p class="section-hdr">Platform Capabilities</p>', unsafe_allow_html=True)
features = [
    ("🤖", "AI Agent Chat",       "Ask natural-language questions about portfolio risk, holdings, and forecasts."),
    ("📊", "Risk Dashboard",      "Interactive VaR, stress tests, and sector exposure charts by manager."),
    ("👥", "Portfolio Management", "View and manage portfolio managers, holdings, and strategy constraints."),
    ("⚙️", "Data Management",     "Monitor Bronze / Silver / Gold layer health and data freshness."),
]
cols = st.columns(4)
for col, (icon, title, desc) in zip(cols, features):
    col.markdown(f'''
    <div class="feat-card">
        <div class="feat-icon">{icon}</div>
        <div class="feat-title">{title}</div>
        <div class="feat-desc">{desc}</div>
    </div>
    ''', unsafe_allow_html=True)

# ── Architecture ─────────────────────────────────────────────────────────────
st.markdown('<p class="section-hdr">Architecture</p>', unsafe_allow_html=True)
st.markdown("""
| Layer | Technology | Key Tables |
|-------|-----------|-----------|
| **Bronze** | Auto Loader, Delta Lake | `stock_prices_bronze`, `macro_indicators_bronze` |
| **Silver** | DQ rules, expectations | `stock_prices`, `macro_indicators`, `forecast_features_daily` |
| **Gold** | Risk engine, ML forecasts | `portfolio_risk_metrics`, `stress_test_results`, `stock_forecasts` |
| **Agent** | LangGraph ReAct, 8 UC tools | Served via Model Serving endpoint |
""")

st.markdown("---")
st.caption("RiskBricks · Built on Databricks Lakehouse · Medallion Architecture · Unity Catalog")
