"""
Portfolio Management — Add and manage portfolio managers and holdings
"""

import streamlit as st
import pandas as pd
import uuid
import os, sys
from datetime import datetime

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from db_utils import run_query, run_statement, CATALOG

st.set_page_config(page_title="Portfolio Management", page_icon="\U0001f465", layout="wide")
st.title("\U0001f465 Portfolio Management")
st.caption("View managers, add new ones, and manage individual holdings.")

# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def get_managers():
    return run_query(f"""
        SELECT m.manager_id, m.manager_name, m.risk_profile, m.strategy_description,
               m.aum_usd AS total_value,
               m.target_return_pct, m.max_volatility_pct, m.beta_min, m.beta_max, m.created_date
        FROM {CATALOG}.gold.portfolio_managers m
         ORDER BY m.manager_name
    """)

@st.cache_data(ttl=120)
def get_holdings(manager_id: str):
    return run_query(f"""
        SELECT h.symbol, c.company_name, c.sector, h.value_usd, h.weight, c.beta, c.volatility_30d
        FROM {CATALOG}.gold.portfolio_holdings h
        JOIN {CATALOG}.gold.company_universe c ON h.symbol = c.symbol
        WHERE h.manager_id = '{_sanitize_id(manager_id)}' ORDER BY h.value_usd DESC
    """)

@st.cache_data(ttl=600)
def get_available_stocks():
    return run_query(f"""
        SELECT symbol, company_name, sector, beta, volatility_30d
        FROM {CATALOG}.gold.company_universe ORDER BY symbol
    """)


def _sanitize_id(val: str) -> str:
    """Sanitize an identifier to prevent SQL injection — allow only alphanumeric + hyphens."""
    import re
    return re.sub(r"[^a-zA-Z0-9\-_]", "", str(val))


def _sanitize_text(val: str) -> str:
    """Escape single quotes in text values to prevent SQL injection."""
    return str(val).replace("'", "''").replace(";", "").replace("--", "")


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["\U0001f4cb View Managers", "\u2795 Add Manager", "\U0001f4ca Manage Holdings"])

managers_df = get_managers()

with tab1:
    if managers_df.empty:
        st.info("No managers found. Create one in the **Add Manager** tab.")
    else:
        for _, m in managers_df.iterrows():
            badge_color = {"Conservative": "\U0001f7e2", "Balanced": "\U0001f7e1", "Aggressive": "\U0001f534"}.get(m["risk_profile"], "\u26aa")
            with st.expander(f"{badge_color} {m['manager_name']} — {m['risk_profile']}"):
                c1, c2, c3 = st.columns(3)
                c1.metric("AUM", f"${m['total_value']/1e6:.1f}M")
                c2.metric("Target Return", f"{m['target_return_pct']:.1f}%")
                c3.metric("Max Vol", f"{m['max_volatility_pct']:.1f}%")
                st.markdown(f"**Strategy:** {m['strategy_description']}")

                hdf = get_holdings(m["manager_id"])
                if not hdf.empty:
                    st.dataframe(
                        hdf.style.format({
                            "value_usd": "${:,.0f}", "weight": "{:.2%}",
                            "beta": "{:.2f}", "volatility_30d": "{:.2%}",
                        }),
                        use_container_width=True, hide_index=True,
                    )
                else:
                    st.caption("No holdings yet.")

with tab2:
    with st.form("add_mgr"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Manager Name *")
        profile = c2.selectbox("Risk Profile *", ["Conservative", "Balanced", "Aggressive"])
        strategy = st.text_area("Strategy Description")
        c1, c2 = st.columns(2)
        target_ret = c1.number_input("Target Return %", 0.0, 50.0, 8.0)
        max_vol = c2.number_input("Max Volatility %", 0.0, 100.0, 15.0)
        submitted = st.form_submit_button("Create Manager", type="primary")

    if submitted and name:
        mid = str(uuid.uuid4())[:8]
        beta_map = {"Conservative": (0.6, 0.9), "Balanced": (0.9, 1.1), "Aggressive": (1.2, 1.8)}
        bmin, bmax = beta_map[profile]

        # SAFE: sanitized inputs prevent SQL injection
        safe_name = _sanitize_text(name)
        safe_strategy = _sanitize_text(strategy)
        safe_profile = _sanitize_text(profile)

        ok = run_statement(f"""
            INSERT INTO {CATALOG}.gold.portfolio_managers
            (manager_id, manager_name, risk_profile, strategy_description,
             target_return_pct, max_volatility_pct, beta_min, beta_max, created_date)
            VALUES ('{mid}', '{safe_name}', '{safe_profile}', '{safe_strategy}',
                    {target_ret}, {max_vol}, {bmin}, {bmax}, current_date())
        """)
        if ok:
            st.success(f"\u2705 Manager **{name}** created!")
            st.cache_data.clear()

with tab3:
    if managers_df.empty:
        st.info("Create a manager first.")
    else:
        sel_mgr = st.selectbox("Select Manager", managers_df["manager_name"].tolist())
        sel_row = managers_df[managers_df["manager_name"] == sel_mgr].iloc[0]
        mgr_id = sel_row["manager_id"]

        stocks = get_available_stocks()
        if stocks.empty:
            st.warning("Stock universe is empty.")
        else:
            with st.form("add_holding"):
                sym = st.selectbox("Stock", stocks["symbol"].tolist())
                c1, c2 = st.columns(2)
                val = c1.number_input("Value (USD)", 1000, 50_000_000, 100_000)
                wt = c2.number_input("Weight", 0.001, 1.0, 0.05, step=0.01)
                add = st.form_submit_button("Add Holding", type="primary")

            if add:
                sector = stocks.loc[stocks["symbol"] == sym, "sector"].values
                sec = _sanitize_text(sector[0]) if len(sector) else "Unknown"
                safe_mgr_id = _sanitize_id(mgr_id)
                safe_sym = _sanitize_id(sym)

                ok = run_statement(f"""
                    INSERT INTO {CATALOG}.gold.portfolio_holdings
                    (manager_id, symbol, sector, value_usd, weight, purchase_date, as_of_date, updated_at)
                    VALUES ('{safe_mgr_id}', '{safe_sym}', '{sec}', {val}, {wt}, current_date(), current_date(), current_timestamp())
                """)
                if ok:
                    st.success(f"\u2705 Added {sym} to {sel_mgr}")
                    st.cache_data.clear()
