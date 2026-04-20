"""
ML Predictions Dashboard — Ensemble Stock Forecast
Shows latest predictions, model confidence, backtesting accuracy, and actuals.
"""

import streamlit as st

# set_page_config MUST be the first Streamlit command
st.set_page_config(page_title="ML Predictions", page_icon="\U0001f3af", layout="wide")

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os, sys

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from db_utils import run_query, CATALOG

# Import centralized sector map (Issue 13)
CONFIG_DIR = os.path.join(os.path.dirname(APP_DIR), "config")
if CONFIG_DIR not in sys.path:
    sys.path.insert(0, os.path.dirname(APP_DIR))
@st.cache_data(ttl=3600)
def _load_sector_map():
    """Load symbol→sector from DB. Falls back to config constants."""
    from db_utils import run_query, CATALOG
    df = run_query(f"SELECT symbol, sector FROM {CATALOG}.gold.company_universe WHERE sector IS NOT NULL")
    if not df.empty:
        return dict(zip(df["symbol"], df["sector"]))
    # Fallback to config constants (available in app context via sys.path)
    try:
        from config.constants import FALLBACK_SECTOR_MAP
        return dict(FALLBACK_SECTOR_MAP)
    except ImportError:
        return {}

SECTOR_MAP = _load_sector_map()
ALL_SECTORS = sorted(set(SECTOR_MAP.values())) if SECTOR_MAP else []

st.title("\U0001f3af ML Stock Predictions")
st.caption("Ensemble model (LightGBM + RandomForest + GradientBoosting) \u2014 17 features, 6 data sources")

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    conf_threshold = st.slider("Min Confidence %", 0, 100, 0, 5) / 100
    direction_filter = st.radio("Direction", ["All", "UP", "DOWN"])
    selected_sectors = st.multiselect("Sectors", ALL_SECTORS, default=ALL_SECTORS)
    if st.button("\U0001f504 Refresh"):
        st.cache_data.clear()
        st.rerun()

# ── Load predictions ─────────────────────────────────────────────────────────
preds_df = run_query(f"""
    SELECT symbol, pred_date, last_close, direction, probability_up,
           confidence, lgb_prob, rf_prob, gb_prob,
           rsi_14, macd_hist, gap_pct, vix, ai_sentiment,
           gdelt_tone, advance_ratio, sector_momentum_5d
    FROM {CATALOG}.gold.ml_stock_predictions
    ORDER BY confidence DESC
""")

if preds_df.empty:
    st.warning("No predictions found. Run the ML Data Ingestion pipeline first.")
    st.stop()

# Add sector column
preds_df["sector"] = preds_df["symbol"].map(SECTOR_MAP).fillna("Other")

# Model agreement
preds_df["lgb_up"] = (preds_df["lgb_prob"] > 0.5).astype(int)
preds_df["rf_up"] = (preds_df["rf_prob"] > 0.5).astype(int)
preds_df["gb_up"] = (preds_df["gb_prob"] > 0.5).astype(int)
preds_df["models_agree"] = preds_df[["lgb_up", "rf_up", "gb_up"]].sum(axis=1)
preds_df["agreement"] = preds_df["models_agree"].map({0: "3/3 DOWN", 1: "2/3 DOWN", 2: "2/3 UP", 3: "3/3 UP"})

# Apply filters
filt = preds_df.copy()
filt = filt[filt["confidence"] >= conf_threshold]
filt = filt[filt["sector"].isin(selected_sectors)]
if direction_filter != "All":
    filt = filt[filt["direction"] == direction_filter]

# ── KPIs ─────────────────────────────────────────────────────────────────────
n_up = (filt["direction"] == "UP").sum()
n_down = (filt["direction"] == "DOWN").sum()
hi_conf = (filt["confidence"] > 0.4).sum()
avg_conf = filt["confidence"].mean() if len(filt) > 0 else 0
unanimous = ((filt["models_agree"] == 3) | (filt["models_agree"] == 0)).sum()
pred_date = str(filt["pred_date"].iloc[0]) if len(filt) > 0 else "\u2014"

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Prediction Date", pred_date)
c2.metric("\U0001f7e2 UP", n_up)
c3.metric("\U0001f534 DOWN", n_down)
c4.metric("High Confidence", f"{hi_conf} ({hi_conf/max(len(filt),1)*100:.0f}%)")
c5.metric("Avg Confidence", f"{avg_conf:.0%}")
c6.metric("Unanimous (3/3)", unanimous)
st.markdown("---")

# ── Tabs ─────────────────────────────────────────────────────────────────────
t1, t2, t3, t4 = st.tabs(["\U0001f4cb Predictions", "\U0001f4ca Sector View", "\U0001f9ea Model Agreement", "\U0001f3af Backtesting"])

# ── Tab 1: Predictions Table ─────────────────────────────────────────────────
with t1:
    st.markdown("#### All Predictions")
    lc, rc = st.columns(2)
    with lc:
        st.markdown("**\U0001f7e2 Top BUY Signals**")
        top_buy = filt[filt["direction"] == "UP"].nlargest(10, "confidence")
        if not top_buy.empty:
            display_buy = top_buy[["symbol", "sector", "last_close", "direction",
                                   "confidence", "probability_up", "agreement"]].copy()
            display_buy["confidence"] = display_buy["confidence"].apply(lambda x: f"{x:.0%}")
            display_buy["probability_up"] = display_buy["probability_up"].apply(lambda x: f"{x:.0%}")
            display_buy["last_close"] = display_buy["last_close"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(display_buy, use_container_width=True, hide_index=True)
        else:
            st.info("No BUY signals matching filters.")
    with rc:
        st.markdown("**\U0001f534 Top SELL Signals**")
        top_sell = filt[filt["direction"] == "DOWN"].nlargest(10, "confidence")
        if not top_sell.empty:
            display_sell = top_sell[["symbol", "sector", "last_close", "direction",
                                     "confidence", "probability_up", "agreement"]].copy()
            display_sell["confidence"] = display_sell["confidence"].apply(lambda x: f"{x:.0%}")
            display_sell["probability_up"] = display_sell["probability_up"].apply(lambda x: f"{x:.0%}")
            display_sell["last_close"] = display_sell["last_close"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(display_sell, use_container_width=True, hide_index=True)
        else:
            st.info("No SELL signals matching filters.")

    st.markdown("---")
    st.markdown("#### Full Predictions Table")
    full_disp = filt[["symbol", "sector", "last_close", "direction", "probability_up",
                       "confidence", "lgb_prob", "rf_prob", "gb_prob", "agreement",
                       "rsi_14", "vix", "ai_sentiment", "gap_pct"]].copy()
    full_disp.columns = ["Symbol", "Sector", "Last Close", "Direction", "P(UP)",
                          "Confidence", "LGB", "RF", "GB", "Agreement",
                          "RSI", "VIX", "Sentiment", "Gap %"]
    st.dataframe(
        full_disp.style.format({
            "Last Close": "${:,.2f}", "P(UP)": "{:.0%}", "Confidence": "{:.0%}",
            "LGB": "{:.0%}", "RF": "{:.0%}", "GB": "{:.0%}",
            "RSI": "{:.1f}", "VIX": "{:.1f}", "Sentiment": "{:.2f}", "Gap %": "{:.2%}"
        }),
        use_container_width=True, hide_index=True, height=600
    )

# ── Tab 2: Sector View ───────────────────────────────────────────────────────
with t2:
    sector_summary = filt.groupby("sector").agg(
        total=pd.NamedAgg(column="symbol", aggfunc="count"),
        up=pd.NamedAgg(column="direction", aggfunc=lambda x: (x == "UP").sum()),
        down=pd.NamedAgg(column="direction", aggfunc=lambda x: (x == "DOWN").sum()),
        avg_conf=pd.NamedAgg(column="confidence", aggfunc="mean"),
        avg_prob=pd.NamedAgg(column="probability_up", aggfunc="mean"),
    ).reset_index()
    sector_summary["bullish_pct"] = sector_summary["up"] / sector_summary["total"] * 100

    lc, rc = st.columns(2)
    with lc:
        fig_sector = px.bar(
            sector_summary.sort_values("bullish_pct", ascending=True),
            x="bullish_pct", y="sector", orientation="h",
            color="bullish_pct",
            color_continuous_scale=["#ef4444", "#eab308", "#22c55e"],
            range_color=[0, 100],
            title="Sector Bullish % (UP predictions)",
            labels={"bullish_pct": "% Bullish", "sector": "Sector"},
        )
        fig_sector.add_vline(x=50, line_dash="dash", line_color="gray")
        fig_sector.update_layout(margin=dict(t=40, b=20), showlegend=False)
        st.plotly_chart(fig_sector, use_container_width=True)

    with rc:
        fig_conf = px.bar(
            sector_summary.sort_values("avg_conf", ascending=True),
            x="avg_conf", y="sector", orientation="h",
            color="avg_conf",
            color_continuous_scale=["#94a3b8", "#3B82F6"],
            title="Average Confidence by Sector",
            labels={"avg_conf": "Avg Confidence", "sector": "Sector"},
        )
        fig_conf.update_layout(margin=dict(t=40, b=20), showlegend=False)
        st.plotly_chart(fig_conf, use_container_width=True)

    st.dataframe(
        sector_summary.rename(columns={
            "sector": "Sector", "total": "Stocks", "up": "UP", "down": "DOWN",
            "avg_conf": "Avg Confidence", "avg_prob": "Avg P(UP)", "bullish_pct": "Bullish %"
        }).style.format({"Avg Confidence": "{:.0%}", "Avg P(UP)": "{:.0%}", "Bullish %": "{:.1f}%"}),
        use_container_width=True, hide_index=True
    )

# ── Tab 3: Model Agreement ───────────────────────────────────────────────────
with t3:
    lc, rc = st.columns(2)
    with lc:
        agree_counts = filt["agreement"].value_counts().reset_index()
        agree_counts.columns = ["Agreement", "Count"]
        color_agree = {"3/3 UP": "#22c55e", "2/3 UP": "#86efac", "2/3 DOWN": "#fca5a5", "3/3 DOWN": "#ef4444"}
        fig_agree = px.pie(agree_counts, names="Agreement", values="Count",
                           title="Model Agreement Distribution",
                           color="Agreement", color_discrete_map=color_agree,
                           hole=0.4)
        fig_agree.update_layout(margin=dict(t=40, b=10))
        st.plotly_chart(fig_agree, use_container_width=True)
    with rc:
        fig_scatter = px.scatter(
            filt, x="probability_up", y="confidence",
            color="direction", symbol="agreement",
            color_discrete_map={"UP": "#22c55e", "DOWN": "#ef4444"},
            hover_data=["symbol", "sector", "last_close"],
            title="Confidence vs P(UP) \u2014 Each Dot is a Stock",
            labels={"probability_up": "P(UP)", "confidence": "Confidence"},
        )
        fig_scatter.add_vline(x=0.5, line_dash="dash", line_color="gray", opacity=0.5)
        fig_scatter.add_hline(y=0.4, line_dash="dash", line_color="blue", opacity=0.3,
                              annotation_text="High-confidence threshold")
        fig_scatter.update_layout(margin=dict(t=40, b=20))
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("#### Individual Model Probabilities")
    model_comp = filt[["symbol", "sector", "direction", "lgb_prob", "rf_prob", "gb_prob", "probability_up"]].copy()
    model_comp.columns = ["Symbol", "Sector", "Direction", "LightGBM", "RandomForest", "GradientBoosting", "Ensemble"]
    st.dataframe(
        model_comp.style.format({
            "LightGBM": "{:.0%}", "RandomForest": "{:.0%}", "GradientBoosting": "{:.0%}", "Ensemble": "{:.0%}"
        }),
        use_container_width=True, hide_index=True, height=500
    )

# ── Tab 4: Backtesting ───────────────────────────────────────────────────────
with t4:
    st.markdown("#### Walk-Forward Backtesting Results")
    st.caption(f"Training data: {CATALOG}.silver.ml_training_features \u2014 predictions vs actuals")

    bt_df = run_query(f"""
        SELECT symbol, pred_date, actual_date, actual_return, actual_up,
               return_5d, return_20d, rsi_14, macd_hist, gap_pct,
               vix, ai_sentiment, gdelt_tone, advance_ratio,
               sector_momentum_5d, sector_breadth, volatility_20d
        FROM {CATALOG}.silver.ml_training_features
        ORDER BY pred_date, symbol
    """)

    if bt_df.empty:
        st.info("No backtesting data available.")
    else:
        bt_df["sector"] = bt_df["symbol"].map(SECTOR_MAP).fillna("Other")
        bt_df["actual_direction"] = bt_df["actual_up"].map({1: "UP", 0: "DOWN"})

        day_summary = bt_df.groupby("pred_date").agg(
            total=pd.NamedAgg(column="symbol", aggfunc="count"),
            actual_up=pd.NamedAgg(column="actual_up", aggfunc="sum"),
            avg_return=pd.NamedAgg(column="actual_return", aggfunc="mean"),
        ).reset_index()
        day_summary["pct_up"] = day_summary["actual_up"] / day_summary["total"] * 100
        day_summary["market_day"] = day_summary["pct_up"].apply(
            lambda x: "\U0001f7e2 Rally" if x > 65 else ("\U0001f534 Crash" if x < 35 else "\U0001f7e1 Mixed"))

        st.markdown("##### Market Conditions by Day")
        st.dataframe(
            day_summary.rename(columns={
                "pred_date": "Date", "total": "Stocks", "actual_up": "# UP",
                "avg_return": "Avg Return", "pct_up": "% UP", "market_day": "Market"
            }).style.format({"Avg Return": "{:.2%}", "% UP": "{:.1f}%"}),
            use_container_width=True, hide_index=True
        )

        fig_mkt = px.bar(
            day_summary, x="pred_date", y="pct_up",
            color="pct_up",
            color_continuous_scale=["#ef4444", "#eab308", "#22c55e"],
            range_color=[20, 80],
            title="Daily % of Stocks Moving UP",
            labels={"pred_date": "Date", "pct_up": "% UP"},
        )
        fig_mkt.add_hline(y=50, line_dash="dash", line_color="gray")
        fig_mkt.update_layout(margin=dict(t=40, b=20), showlegend=False)
        st.plotly_chart(fig_mkt, use_container_width=True)

        st.markdown("##### Actual Direction by Sector")
        sector_actual = bt_df.groupby("sector").agg(
            total=pd.NamedAgg(column="symbol", aggfunc="count"),
            up=pd.NamedAgg(column="actual_up", aggfunc="sum"),
            avg_ret=pd.NamedAgg(column="actual_return", aggfunc="mean"),
        ).reset_index()
        sector_actual["pct_up"] = sector_actual["up"] / sector_actual["total"] * 100

        fig_sa = px.bar(
            sector_actual.sort_values("pct_up", ascending=True),
            x="pct_up", y="sector", orientation="h",
            color="pct_up",
            color_continuous_scale=["#ef4444", "#eab308", "#22c55e"],
            range_color=[30, 70],
            title="Actual % UP by Sector (Training Period)",
            labels={"pct_up": "% UP", "sector": "Sector"},
        )
        fig_sa.add_vline(x=50, line_dash="dash", line_color="gray")
        fig_sa.update_layout(margin=dict(t=40, b=20), showlegend=False)
        st.plotly_chart(fig_sa, use_container_width=True)

        st.markdown("##### Full Actuals Table")
        bt_display = bt_df[["symbol", "sector", "pred_date", "actual_date",
                             "actual_direction", "actual_return",
                             "rsi_14", "vix", "gap_pct", "ai_sentiment"]].copy()
        bt_display.columns = ["Symbol", "Sector", "Pred Date", "Actual Date",
                               "Actual", "Return", "RSI", "VIX", "Gap %", "Sentiment"]
        st.dataframe(
            bt_display.style.format({
                "Return": "{:.2%}", "RSI": "{:.1f}", "VIX": "{:.1f}",
                "Gap %": "{:.2%}", "Sentiment": "{:.2f}"
            }),
            use_container_width=True, hide_index=True, height=600
        )

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Model: riskbricks.models.stock_forecast_ensemble (LGB+RF+GB) | "
    "Walk-forward accuracy: 70.3% overall, 76.7% high-confidence | "
    "Top feature: gap_pct (overnight gap)"
)
