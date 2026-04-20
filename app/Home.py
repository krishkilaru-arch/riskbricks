"""
RiskBricks — AI-Powered Portfolio Risk Analytics
"""

import streamlit as st
import pandas as pd
import os, sys

APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from db_utils import run_query, CATALOG

st.set_page_config(page_title="RiskBricks", page_icon="\U0001f4ca", layout="wide")

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
    <h1>\U0001f4ca RiskBricks</h1>
    <p>AI-Powered Portfolio Risk Analytics Platform</p>
</div>
""", unsafe_allow_html=True)

# ── Live metrics ─────────────────────────────────────────────────────────────
managers_df = run_query(f"""
    SELECT manager_name, risk_profile, aum_usd
    FROM {CATALOG}.gold.portfolio_managers
""")
holdings_cnt = run_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.portfolio_holdings")
universe_cnt = run_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.company_universe")
freshness    = run_query(f"SELECT MAX(date) AS latest FROM {CATALOG}.silver.stock_prices")
risk_df      = run_query(f"""
    SELECT manager_name, var_1day_95_usd, portfolio_beta,
           weighted_volatility_pct, aum_usd
    FROM {CATALOG}.gold.portfolio_risk_metrics
""")

# ML prediction summary
ml_summary = run_query(f"""
    SELECT direction, COUNT(*) AS cnt, AVG(confidence) AS avg_conf
    FROM {CATALOG}.gold.ml_stock_predictions
    GROUP BY direction
""")

has_data = not managers_df.empty

if has_data:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total AUM",     f"${managers_df['aum_usd'].sum()/1e6:,.1f}M")
    c2.metric("Managers",       len(managers_df))
    c3.metric("Positions",      f"{int(holdings_cnt.iloc[0]['cnt']):,}" if not holdings_cnt.empty else "\u2014")
    c4.metric("Stock Universe", f"{int(universe_cnt.iloc[0]['cnt']):,}" if not universe_cnt.empty else "\u2014")
    c5.metric("Data As-Of",     str(freshness.iloc[0]["latest"]) if not freshness.empty else "\u2014")
else:
    st.info("Connecting to data\u2026 If this persists, check the SQL warehouse resource in App settings.")

# ML prediction quick KPIs
if not ml_summary.empty:
    st.markdown('<p class="section-hdr">\U0001f3af ML Direction Snapshot</p>', unsafe_allow_html=True)
    up_row = ml_summary[ml_summary["direction"] == "UP"]
    dn_row = ml_summary[ml_summary["direction"] == "DOWN"]
    n_up = int(up_row["cnt"].iloc[0]) if not up_row.empty else 0
    n_dn = int(dn_row["cnt"].iloc[0]) if not dn_row.empty else 0
    avg_c = ml_summary["avg_conf"].mean()
    sentiment = "BULLISH" if n_up > n_dn else "BEARISH"
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("ML Sentiment", sentiment)
    mc2.metric("\U0001f7e2 UP Predictions", n_up)
    mc3.metric("\U0001f534 DOWN Predictions", n_dn)
    mc4.metric("Avg Confidence", f"{avg_c:.0%}")

# ── Risk snapshot ────────────────────────────────────────────────────────────
if has_data and not risk_df.empty:
    st.markdown('<p class="section-hdr">Risk Snapshot \u2014 Manager Comparison</p>', unsafe_allow_html=True)
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
    ("\U0001f916", "AI Agent Chat",       "Ask natural-language questions about portfolio risk, holdings, and forecasts."),
    ("\U0001f4ca", "Risk Dashboard",      "Interactive VaR, stress tests, and sector exposure charts by manager."),
    ("\U0001f465", "Portfolio Management", "View and manage portfolio managers, holdings, and strategy constraints."),
    ("\u2699\ufe0f", "Data Management",     "Monitor Bronze / Silver / Gold layer health and data freshness."),
    ("\U0001f3af", "ML Predictions",      "Ensemble stock direction forecasts with confidence scores, sector analysis, and backtesting."),
    ("\U0001f4d6", "About & Docs",        "Full architecture diagrams, evaluation results, and Databricks feature coverage."),
]
cols = st.columns(6)
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
|-------|-----------|-----------:|
| **Bronze** | Auto Loader, Delta Lake | `stock_prices_bronze`, `fred_macro_indicators`, `news_rss_all` |
| **Silver** | DQ rules, expectations | `stock_prices`, `technical_indicators`, `sector_features`, `market_breadth` |
| **Gold** | Risk engine, ML forecasts | `portfolio_risk_metrics`, `ml_stock_predictions`, `ml_prediction_features` |
| **ML** | LGB+RF+GB Ensemble | `models.stock_forecast_ensemble` (17 features, 70.3% accuracy) |
| **Agent** | LangGraph ReAct, 11 UC tools | Served via Model Serving endpoint |
""")

st.markdown("---")
st.caption("RiskBricks \u00b7 Built on Databricks Lakehouse \u00b7 Databricks Summit 2026 \u00b7 See \U0001f4d6 About page for full documentation")
