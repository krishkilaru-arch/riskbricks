"""
Risk Dashboard — Interactive risk analytics and visualizations
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import os, sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from db_utils import run_query, CATALOG

st.set_page_config(page_title="Risk Dashboard", page_icon="\U0001f4ca", layout="wide")
st.title("\U0001f4ca Risk Dashboard")
st.caption("Comprehensive risk analytics and visualizations for all portfolio managers.")

# ── Fetch core data ──────────────────────────────────────────────────────────
risk_df = run_query(f"""
    SELECT manager_name, risk_profile, aum_usd, portfolio_beta,
           weighted_volatility_pct, var_1day_95_usd, var_10day_95_usd, num_positions
    FROM {CATALOG}.gold.portfolio_risk_metrics ORDER BY aum_usd DESC
""")
stress_df = run_query(f"""
    SELECT manager_name, scenario_name, scenario_description,
           total_impact_usd, impact_pct
    FROM {CATALOG}.gold.stress_test_results ORDER BY ABS(impact_pct) DESC
""")
sector_df = run_query(f"""
    SELECT manager_name, sector, sector_weight_pct
    FROM {CATALOG}.gold.sector_exposures ORDER BY manager_name, sector_weight_pct DESC
""")

if risk_df.empty:
    st.warning("No risk metrics found. Please run the analytics pipeline first.")
    st.stop()

# ── Sidebar filters ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    all_mgrs = risk_df["manager_name"].unique().tolist()
    selected = st.multiselect("Managers", all_mgrs, default=all_mgrs)
    view = st.radio("View", ["Absolute ($)", "% of AUM"])
    if st.button("\U0001f504 Refresh"):
        st.cache_data.clear()
        st.rerun()

filt_risk   = risk_df[risk_df["manager_name"].isin(selected)].copy()
filt_stress = stress_df[stress_df["manager_name"].isin(selected)].copy()
filt_sector = sector_df[sector_df["manager_name"].isin(selected)].copy()

# ── Overview KPIs ────────────────────────────────────────────────────────────
total_aum = filt_risk["aum_usd"].sum()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total AUM", f"${total_aum/1e6:,.1f}M")
c2.metric("Wtd Avg Beta", f"{(filt_risk['portfolio_beta'] * filt_risk['aum_usd']).sum() / max(total_aum, 1):.2f}")
c3.metric("1-Day VaR", f"${filt_risk['var_1day_95_usd'].sum()/1e6:,.2f}M")
c4.metric("VaR / AUM", f"{filt_risk['var_1day_95_usd'].sum() / max(total_aum, 1) * 100:.2f}%")
st.markdown("---")

# ── Tabs ─────────────────────────────────────────────────────────────────────
t1, t2, t3, t4 = st.tabs(["VaR Analysis", "Stress Tests", "Sector Exposure", "Signals & Forecasts"])

color_map = {"Conservative": "#22c55e", "Balanced": "#eab308", "Aggressive": "#ef4444"}

# ── Tab 1: VaR ───────────────────────────────────────────────────────────────
with t1:
    lc, rc = st.columns(2)
    with lc:
        if view == "% of AUM":
            filt_risk["var1_pct"] = filt_risk["var_1day_95_usd"] / filt_risk["aum_usd"] * 100
            fig = px.bar(filt_risk, x="manager_name", y="var1_pct", color="risk_profile",
                         title="1-Day VaR as % of AUM", color_discrete_map=color_map)
        else:
            fig = px.bar(filt_risk, x="manager_name", y="var_1day_95_usd", color="risk_profile",
                         title="1-Day VaR (95%)", color_discrete_map=color_map)
        fig.update_layout(margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)
    with rc:
        if view == "% of AUM":
            filt_risk["var10_pct"] = filt_risk["var_10day_95_usd"] / filt_risk["aum_usd"] * 100
            fig2 = px.bar(filt_risk, x="manager_name", y="var10_pct", color="risk_profile",
                          title="10-Day VaR as % of AUM", color_discrete_map=color_map)
        else:
            fig2 = px.bar(filt_risk, x="manager_name", y="var_10day_95_usd", color="risk_profile",
                          title="10-Day VaR (95%)", color_discrete_map=color_map)
        fig2.update_layout(margin=dict(t=40, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(filt_risk[["manager_name", "risk_profile", "aum_usd", "portfolio_beta",
                             "weighted_volatility_pct", "var_1day_95_usd", "var_10day_95_usd"]].style.format({
        "aum_usd": "${:,.0f}", "var_1day_95_usd": "${:,.0f}", "var_10day_95_usd": "${:,.0f}",
        "portfolio_beta": "{:.2f}", "weighted_volatility_pct": "{:.1f}%"
    }), use_container_width=True, hide_index=True)

# ── Tab 2: Stress Tests ──────────────────────────────────────────────────────
with t2:
    if not filt_stress.empty:
        fig3 = px.bar(filt_stress, x="scenario_name", y="impact_pct", color="manager_name",
                      barmode="group", title="Stress Test Impact (% of Portfolio)")
        fig3.update_layout(margin=dict(t=40, b=20))
        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(filt_stress.style.format({
            "total_impact_usd": "${:,.0f}", "impact_pct": "{:.2f}%"
        }), use_container_width=True, hide_index=True)
    else:
        st.info("No stress test data available.")

# ── Tab 3: Sector Exposure ───────────────────────────────────────────────────
with t3:
    if not filt_sector.empty:
        fig4 = px.sunburst(filt_sector, path=["manager_name", "sector"], values="sector_weight_pct",
                           title="Sector Allocation by Manager")
        fig4.update_layout(margin=dict(t=40, b=10))
        st.plotly_chart(fig4, use_container_width=True)
        for mgr in selected:
            mfx = filt_sector[filt_sector["manager_name"] == mgr]
            if not mfx.empty:
                fig5 = px.pie(mfx, names="sector", values="sector_weight_pct",
                              title=f"{mgr} — Sector Weights", hole=0.4)
                fig5.update_layout(margin=dict(t=40, b=10))
                st.plotly_chart(fig5, use_container_width=True)
    else:
        st.info("No sector exposure data available.")

# ── Tab 4: Signals & Forecasts ───────────────────────────────────────────────
with t4:
    st.markdown("#### Decision Signals")
    signals_df = run_query(f"""
        SELECT symbol, as_of_date, target_date, signal, score, expected_return
        FROM {CATALOG}.gold.decision_signals
        ORDER BY as_of_date DESC
    """)
    if not signals_df.empty:
        st.dataframe(signals_df, use_container_width=True, hide_index=True)
    else:
        st.info("No decision signals available.")

    st.markdown("#### Accuracy Scoreboard")
    scoreboard_df = run_query(f"""
        SELECT symbol, horizon_days, window_start, window_end,
               hit_rate, mae, rmse, mape, sample_size
        FROM {CATALOG}.gold.accuracy_scoreboard_daily
        ORDER BY window_end DESC, hit_rate DESC
    """)
    if not scoreboard_df.empty:
        st.dataframe(scoreboard_df, use_container_width=True, hide_index=True)
    else:
        st.info("No accuracy scoreboard data available.")
