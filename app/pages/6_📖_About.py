"""
RiskBricks — About & Documentation
Renders project documentation inside the Streamlit app for Summit demos.
"""

import streamlit as st

st.set_page_config(page_title="RiskBricks · About", page_icon="📖", layout="wide")

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .about-hero {text-align:center; padding:1.2rem 0 0.4rem;}
    .about-hero h1 {font-size:2.4rem; font-weight:800; margin:0; color:#1B2A4A;}
    .about-hero p  {font-size:1.1rem; color:#6c757d; margin-top:0.2rem;}
    .stat-box {background:#f0f4ff; border-radius:12px; padding:18px 16px;
               text-align:center; border-left:4px solid #3B82F6;}
    .stat-num {font-size:1.8rem; font-weight:800; color:#1B2A4A; margin:0;}
    .stat-lbl {font-size:0.85rem; color:#6c757d; margin:0;}
    .section-title {font-size:1.3rem; font-weight:700; color:#1B2A4A;
                    margin:2rem 0 0.8rem; border-bottom:2px solid #3B82F6;
                    padding-bottom:0.3rem;}
    pre {font-size:0.78rem !important;}
</style>
""", unsafe_allow_html=True)

# ── Hero ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="about-hero">
    <h1>📖 RiskBricks Documentation</h1>
    <p>AI-Powered Portfolio Risk Analytics on Databricks Lakehouse</p>
    <p><em>Databricks Summit 2026</em></p>
</div>
""", unsafe_allow_html=True)

st.markdown("> **One question drives everything:** *\"What is the risk in my portfolio — and what should I do about it?\"*")

# ── Key Metrics ──────────────────────────────────────────────────────────────
st.markdown("---")
c1, c2, c3, c4, c5, c6 = st.columns(6)
metrics = [
    ("40", "Unity Catalog Tables"),
    ("11", "UC Functions"),
    ("6", "AI Sub-Agents"),
    ("31", "ML Features"),
    ("220", "Eval Questions"),
    ("93.2%", "Pass Rate (v31)"),
]
for col, (num, label) in zip([c1, c2, c3, c4, c5, c6], metrics):
    col.markdown(f"""
    <div class="stat-box">
        <p class="stat-num">{num}</p>
        <p class="stat-lbl">{label}</p>
    </div>
    """, unsafe_allow_html=True)

# ── Tabs for organized documentation ─────────────────────────────────────────
tabs = st.tabs([
    "🏗️ Architecture",
    "🗄️ Data Lakehouse",
    "🤖 AI Agents",
    "🧠 ML Model",
    "🧪 Evaluation",
    "⚙️ Databricks Features",
    "🚀 CI/CD",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: Architecture
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[0]:
    st.markdown('<p class="section-title">End-to-End System Architecture</p>', unsafe_allow_html=True)

    st.code("""
╔══════════════════════════════════════════════════════════════════════════════════╗
║                           EXTERNAL DATA SOURCES                                ║
╠════════════════╦════════════════╦════════════════╦════════════════╦═════════════╣
║  Yahoo Finance ║   FRED (Fed)   ║   RSS Feeds    ║     GDELT      ║   Static    ║
║  Stock OHLCV   ║   Macro Data   ║ Yahoo + Google ║  Global Events ║  Portfolios ║
║   (yfinance)   ║   (CSV API)    ║  (feedparser)  ║  (ZIP → TSV)   ║  (Python)   ║
╚═══════╤════════╩═══════╤════════╩═══════╤════════╩═══════╤════════╩══════╤══════╝
        │                │                │                │               │
        ▼                ▼                ▼                ▼               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Raw Ingestion — append-only, schema-on-read)                    │
│  stock_prices_bronze (37K) · fred_macro_indicators (162) · news_rss_all (76K)   │
│  historical_news_gdelt (18.5M) · portfolio_holdings_bronze (430)                │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                   │  validate · deduplicate · enrich
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Cleaned + Feature-Engineered)                                   │
│  stock_prices (1.1M) · technical_indicators · sector_features · market_breadth  │
│  news_ai_sentiment · forecast_features_daily · macro_indicators                 │
│  ml_training_features (408 samples × 31 features)                               │
└──────────┬───────────────────────────────┬───────────────────────────────────────┘
           │                               │
           ▼                               ▼
┌─────────────────────────────┐  ┌────────────────────────────────────────────────┐
│  ML TRAINING PIPELINE       │  │  GOLD LAYER  (Business-Ready Analytics)        │
│  LightGBM + RF + GB        │  │  company_universe · portfolio_holdings          │
│  Walk-Forward CV            │  │  portfolio_risk_metrics · stock_forecasts      │
│  MLflow + UC Registry       │  │  decision_signals · ml_stock_predictions       │
│  70.3% Accuracy             │  │  risk_factor_exposures · sector_exposures      │
└─────────────┬───────────────┘  └──────────────────────┬─────────────────────────┘
              └────────────┬────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  UNITY CATALOG FUNCTIONS  (11 SQL functions in riskbricks.agent_tools)          │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  MULTI-AGENT AI SYSTEM  (LangGraph + Mosaic AI Agent Framework)                 │
│  Supervisor → Risk · Price Target · Factor · Decision · News · ML Direction     │
│  Endpoint: riskbricks-supervisor-agent  ·  LLM: Llama 3.3 70B                  │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STREAMLIT APPLICATION  (Databricks Apps — 5 pages)                             │
└──────────────────────────────────────────────────────────────────────────────────┘
""", language=None)

    st.markdown('<p class="section-title">Agent Routing Flow</p>', unsafe_allow_html=True)

    st.code("""
                      User: "Should I buy NVDA?"
                                │
                                ▼
                   ┌────────────────────────┐
                   │   Supervisor Agent      │
                   │   (Llama 3.3 70B)       │
                   │   Parses intent →       │
                   │   {"next":"decision_.."}│
                   └──────────┬─────────────┘
                              │
                ┌─────────────┼──────────────┐
                ▼             ▼               ▼
        ┌──────────┐  ┌────────────┐  ┌────────────┐
        │ Decision  │  │ ML Direct. │  │  Price     │
        │  Agent    │  │   Agent    │  │  Target    │
        │  ↓       │  │  ↓        │  │  ↓        │
        │ get_     │  │ get_ml_   │  │ get_stock_│
        │ decision_│  │ stock_    │  │ forecast()│
        │ signal() │  │ forecast()│  │           │
        └──────────┘  └────────────┘  └────────────┘
                │             │               │
                └─────────────┼───────────────┘
                              ▼
                "The signal for NVDA is **BUY**
                 with a score of 6.01..."
""", language=None)

    st.markdown('<p class="section-title">Data Flow Summary</p>', unsafe_allow_html=True)
    st.markdown("""
```
4 Sources → 5 Bronze → 8 Silver → 12 Gold → 11 UC Functions → 6 AI Agents → Streamlit App
```
""")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: Data Lakehouse
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.markdown('<p class="section-title">Data Sources</p>', unsafe_allow_html=True)

    st.markdown("""
| Source | API | Volume | Refresh |
|--------|-----|--------|---------|
| **Yahoo Finance** | `yfinance` Python lib | 432 stocks × 90 days OHLCV | Daily |
| **FRED** | `fred.stlouisfed.org` CSV | 8 macro indicators (VIX, yields, oil, USD) | Daily |
| **RSS News** | Yahoo + Google News RSS | 52 symbols × ~1,500 articles | Daily |
| **GDELT** | `data.gdeltproject.org` ZIP/TSV | 18.5M global events | Daily |
| **Portfolios** | Static Python config | 3 managers, 139 positions | On setup |
""")

    st.markdown('<p class="section-title">Bronze Layer — Raw Ingestion</p>', unsafe_allow_html=True)
    st.markdown("""
| Table | Rows | Source | Key Columns |
|-------|------|--------|-------------|
| `stock_prices_bronze` | 37K | Yahoo Finance | symbol, date, open, high, low, close, volume |
| `fred_macro_indicators` | 162 | FRED | indicator, date, value |
| `news_rss_all` | 76K | RSS Feeds | symbol, title, published, source |
| `historical_news_gdelt` | 18.5M | GDELT | date, actor1, actor2, tone, goldstein_scale |
| `portfolio_holdings_bronze` | 430 | Static Config | manager_name, symbol, shares, weight |
""")

    st.markdown('<p class="section-title">Silver Layer — Cleaned & Feature-Engineered</p>', unsafe_allow_html=True)
    st.markdown("""
| Table | Rows | Key Enrichments |
|-------|------|------------------|
| `stock_prices` | 1.1M | price_change_pct, quality_score, anomaly flags |
| `technical_indicators` | — | RSI-14, MACD histogram, Bollinger %B, volume ratio |
| `sector_features` | — | Sector relative momentum, breadth, dispersion |
| `market_breadth` | — | Advance/decline ratio, % above MA20, market dispersion |
| `news_ai_sentiment` | — | AI sentiment score (-1 to +1), positive/negative counts |
| `forecast_features_daily` | 29K | 31-feature matrix for ML training |
| `macro_indicators` | — | Cleaned, forward-filled, with change metrics |
| `ml_training_features` | 408 | Final training matrix with target labels |
""")

    st.markdown('<p class="section-title">Gold Layer — Business-Ready Analytics</p>', unsafe_allow_html=True)
    st.markdown("""
| Table | Rows | Purpose | UC Function |
|-------|------|---------|-----------------------|
| `company_universe` | 432 | Master symbol reference | — |
| `portfolio_holdings` | 139 | Position-level detail | `get_portfolio_holdings` |
| `portfolio_managers` | 3 | Manager profiles | `get_portfolio_holdings` |
| `portfolio_risk_metrics` | 3 | VaR, beta, volatility per manager | `get_portfolio_risk_metrics` |
| `stress_test_results` | 12 | 4 scenarios × 3 managers | `get_stress_test_results` |
| `stock_forecasts` | 104 | 1d/15d predictions with confidence bands | `get_stock_forecast` |
| `decision_signals` | 836 | BUY/HOLD/SELL with scores | `get_decision_signal` |
| `risk_factor_exposures` | 52 | Fama-French betas (market, SMB, HML) | `get_factor_exposures` |
| `sector_exposures` | — | Sector weights per manager | `get_sector_exposures` |
| `macro_indicators_daily` | 82 | Fed Funds, CPI, GDP, VIX, S&P 500 | `get_macro_context` |
| `ml_stock_predictions` | 52 | UP/DOWN with confidence | `get_ml_stock_forecast` |
| `ml_prediction_features` | — | Feature importance | `get_ml_market_overview` |
""")

    st.markdown('<p class="section-title">Three Portfolio Managers</p>', unsafe_allow_html=True)
    st.markdown("""
| Manager | Style | AUM | Positions | Beta |
|---------|-------|-----|-----------|------|
| **Sarah Russel** | Conservative | $90M | 45 | 0.8 |
| **Rena Tang** | Balanced | $72M | 46 | 0.9 |
| **Mohit Arora** | Aggressive | $180M | 48 | 1.0 |
""")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: AI Agents
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[2]:
    st.markdown('<p class="section-title">Supervisor + 6 Sub-Agents</p>', unsafe_allow_html=True)
    st.markdown("Built with **LangGraph** (state graph with conditional routing) and **Mosaic AI Agent Framework** (`ChatAgent` for MLflow-compatible serving).")

    st.markdown("""
| Sub-Agent | Domain | UC Functions | Key Outputs |
|-----------|--------|--------------|-------------|
| **Risk Agent** | Portfolio risk | `get_portfolio_risk_metrics`, `get_stress_test_results` | VaR (1d/10d, 95%), beta, volatility, stress scenarios |
| **Price Target Agent** | Stock forecasting | `get_stock_forecast` | 1d/15d predictions with confidence bands |
| **ML Direction Agent** | Ensemble ML | `get_ml_stock_forecast`, `get_ml_market_overview` | UP/DOWN direction, confidence, model agreement |
| **Factor Agent** | Factor & sector | `get_factor_exposures`, `get_sector_exposures` | Fama-French betas, sector weight breakdowns |
| **Decision Agent** | Investment signals | `get_decision_signal`, `get_macro_context` | BUY/HOLD/SELL with conviction score, macro context |
| **News Agent** | News & holdings | `get_news_context`, `get_portfolio_holdings` | Recent headlines, portfolio position detail |
""")

    st.markdown('<p class="section-title">11 UC Functions</p>', unsafe_allow_html=True)
    st.markdown("""
| Function | Returns |
|----------|---------|
| `get_portfolio_risk_metrics(manager)` | VaR, beta, volatility, AUM |
| `get_stress_test_results(manager)` | 4 scenarios with $ and % impact |
| `get_stock_forecast(symbol)` | 1d/15d price, direction, confidence bands |
| `get_ml_stock_forecast(symbol)` | UP/DOWN, confidence, model agreement |
| `get_ml_market_overview()` | Market sentiment, sector breakdown |
| `get_factor_exposures(symbol)` | Market, SMB, HML betas, alpha |
| `get_sector_exposures(manager)` | Sector weights with % breakdown |
| `get_decision_signal(symbol)` | BUY/HOLD/SELL, score, expected return |
| `get_macro_context()` | Fed Funds, CPI, GDP, VIX, S&P 500 |
| `get_news_context(symbol, sector)` | Recent headlines with sentiment |
| `get_portfolio_holdings(manager)` | Position-level detail with weights |
""")

    st.markdown('<p class="section-title">Key Technical Decisions</p>', unsafe_allow_html=True)
    st.markdown("""
| Decision | Rationale |
|----------|----------|
| **Llama 3.3 70B** | Best open-source instruction-following model on Databricks Foundation APIs; temp=0.1 |
| **LangGraph over LangChain** | State graph enables supervisor routing with re-entry prevention and recursion limits |
| **UC Functions as tools** | SQL on serverless; decouples data access from agent logic; governed by Unity Catalog |
| **Supervisor FINISH suppression** | Critical v31 fix: prevents supervisor summary from burying sub-agent responses |
""")

    st.markdown('<p class="section-title">Production Guardrails</p>', unsafe_allow_html=True)
    st.markdown("""
- **Input validation**: 2000-char length cap + prompt injection detection
- **Output sanitization**: Regex-based redaction of leaked API keys/secrets
- **Re-routing prevention**: Supervisor tracks which agents already responded
- **Structured audit logging**: JSON logs with request_id, latency, agent_route
- **Deduplication**: Sentence-level and content-fingerprint dedup
- **Recursion limit**: MAX_GRAPH_RECURSION = 6 to prevent infinite loops
""")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: ML Model
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[3]:
    st.markdown('<p class="section-title">Ensemble Model Architecture</p>', unsafe_allow_html=True)

    st.code("""
            ml_training_features (408 samples × 31 features)
                                │
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
         ┌────────────┐ ┌────────────┐ ┌────────────────┐
         │  LightGBM  │ │  Random    │ │  Gradient      │
         │  leaves=8  │ │  Forest    │ │  Boosting      │
         │  lr=0.1    │ │  trees=100 │ │  trees=50      │
         │  n_est=50  │ │  depth=5   │ │  depth=3,lr=0.1│
         └──────┬─────┘ └──────┬─────┘ └───────┬────────┘
                └──────────────┼────────────────┘
                               ▼
                      Soft Vote Ensemble
                    (avg probabilities)
                               │
                               ▼
                    UP / DOWN + confidence
                    + model agreement (N/3)
""", language=None)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Overall Accuracy", "70.3%")
    m2.metric("High-Confidence", "76.7%")
    m3.metric("Features", "31")
    m4.metric("Top Feature", "gap_pct")

    st.markdown('<p class="section-title">Feature Groups (31 Features)</p>', unsafe_allow_html=True)
    st.markdown("""
| Group | Count | Features | Source |
|-------|-------|----------|--------|
| **Price/Technical** | 10 | return_5d, return_20d, volatility_20d, rsi_14, macd_hist, bb_pct, vol_ratio, avg_range_5, gap_pct, close_position | Yahoo Finance |
| **Sector** | 4 | sector_rel_5d, sector_momentum_5d, sector_breadth, stock_vs_sector_1d | Sector features |
| **Market** | 4 | market_return, advance_ratio, pct_above_ma20, market_dispersion | Market breadth |
| **Macro** | 3 | vix, hy_spread, treasury_10y | FRED |
| **News/Sentiment** | 4 | ai_sentiment, news_count, pos_articles, neg_articles | RSS |
| **Events** | 2 | gdelt_tone, gdelt_events | GDELT |
| **Calendar** | 4 | days_to_earnings, earnings_within_5d, day_of_week, month | Derived |
""")

    st.markdown('<p class="section-title">MLflow Integration</p>', unsafe_allow_html=True)
    st.markdown("""
- **Experiment**: `riskbricks_stock_forecast` — tracks all training runs
- **Logged Artifacts**: Model pickle, feature importance plot, confusion matrix, walk-forward results
- **Signature**: Inferred from training data — enforced at prediction time
- **Production Alias**: `@production` tag in Unity Catalog Model Registry
- **Registered As**: `riskbricks.models.stock_forecast_ensemble` v1
""")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5: Evaluation
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[4]:
    st.markdown('<p class="section-title">220-Question Evaluation Suite</p>', unsafe_allow_html=True)
    st.markdown("**Location**: `demo/agent_evaluation` notebook — parallel execution, 4 concurrent requests, 180s timeout per question.")

    st.markdown("""
| Check | What It Validates |
|-------|-------------------|
| `has_answer` | Response is non-empty (>10 chars) |
| `no_error` | No generic error messages |
| `has_keyword` | Contains expected keyword from question's keyword list |
| `clean_dollar` | No backslash-escaped dollar signs |
| `has_table` | Response includes markdown table formatting |

**PASS** = `has_answer` AND `no_error` AND `has_keyword`
""")

    st.markdown('<p class="section-title">Version History & Results</p>', unsafe_allow_html=True)
    st.markdown("""
| Category | v23 (baseline) | v26 | v29 | **v31 (current)** | Delta |
|----------|---------------|-----|-----|-------------------|-------|
| **Overall** | **75.9%** | 73.6% | 75.5% | **93.2%** | **+17.3pp** |
| decision_signal | 33.3% | 26.7% | 43.3% | **100.0%** | +66.7pp |
| factor | 90.0% | 65.0% | 75.0% | **100.0%** | +10.0pp |
| ml_forecast | 60.0% | 100.0% | 93.3% | **100.0%** | +40.0pp |
| news | 93.3% | 93.3% | 100.0% | **100.0%** | +6.7pp |
| sector | 100.0% | 100.0% | 100.0% | **100.0%** | stable |
| price_target | 86.7% | 73.3% | 66.7% | **96.7%** | +10.0pp |
| cross_agent | 46.7% | 73.3% | 66.7% | **93.3%** | +46.6pp |
| holdings | 85.0% | 80.0% | 80.0% | **90.0%** | +5.0pp |
| portfolio_risk | 90.0% | 86.7% | 83.3% | **86.7%** | -3.3pp |
| macro | 86.7% | 80.0% | 86.7% | **80.0%** | -6.7pp |
| edge_case | 60.0% | 50.0% | 50.0% | **60.0%** | stable |
""")

    st.markdown('<p class="section-title">Key Fixes That Drove Improvement</p>', unsafe_allow_html=True)
    st.markdown("""
| Version | Fix | Impact |
|---------|-----|--------|
| **v24** | Added "CRITICAL — ALWAYS STATE THE DIRECTION" to ML_DIRECTION_PROMPT | ml_forecast: 60% → 93% |
| **v26** | Added `recommendation` column to UC function + few-shot examples | ml_forecast → 100%, cross_agent: 47% → 73% |
| **v29** | Excluded `delta-spark==3.4.0` from pip requirements (non-existent on PyPI) | Deployment fix |
| **v31** | **Supervisor FINISH suppression** — emit pure JSON instead of LLM summary | **Overall: 75.5% → 93.2%** |
""")

    st.markdown('<p class="section-title">Root Cause: The Supervisor FINISH Bug (v31)</p>', unsafe_allow_html=True)
    st.markdown("The single most impactful fix in the project:")
    st.code("""
Message[0]: [decision_agent]: | Symbol | Signal | ... | NVDA | BUY | 6.01 |   ← HAS SIGNAL ✅
Message[1]: The beta of 1.7 indicates NVDA is volatile. {"next": "FINISH"}    ← NO SIGNAL ❌
                                                                                 ↑ eval picks this
""", language=None)
    st.markdown("The evaluation harness takes `messages[-1]` — which was the supervisor's lossy paraphrase, not the decision agent's data-driven response. **Fix**: suppress the supervisor's text when routing to FINISH.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6: Databricks Features
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[5]:
    st.markdown('<p class="section-title">Databricks Platform Feature Coverage</p>', unsafe_allow_html=True)
    st.markdown("RiskBricks is designed as a comprehensive showcase of the Databricks platform:")

    st.markdown("""
| Feature | How RiskBricks Uses It |
|---------|------------------------|
| **Unity Catalog** | Single `riskbricks` catalog with 7 schemas; table-level governance, column comments, lineage |
| **Medallion Architecture** | Bronze → Silver → Gold with clear lineage |
| **Delta Lake** | MERGE for upserts, time travel for audit, Z-ORDER for performance |
| **MLflow** | Experiment tracking, model logging with signatures, walk-forward CV metrics |
| **UC Model Registry** | `riskbricks.models.stock_forecast_ensemble` with production alias |
| **Model Serving** | `riskbricks-supervisor-agent` endpoint on serverless compute |
| **Foundation Model APIs** | `databricks-meta-llama-3-3-70b-instruct` as LLM backbone |
| **Mosaic AI Agent Framework** | `ChatAgent` wrapper, `mlflow.pyfunc.log_model` for deployment |
| **LangGraph** | Supervisor → sub-agent routing with `StateGraph`, `create_react_agent` |
| **UC Functions (SQL)** | 11 functions in `agent_tools` — tool interface between agents and data |
| **Databricks Apps** | Streamlit app with 5 pages, served via `app.yaml` |
| **Databricks Asset Bundles** | `databricks.yml` with dev/staging/prod targets |
| **GitHub Actions CI/CD** | 4-gate pipeline: lint → unit tests → integration → deploy |
| **Serverless Compute** | Notebooks + UC function execution |
| **Secrets Management** | `dbutils.secrets.get(scope="riskbricks", key="fred-api-key")` |
| **Widgets** | `dbutils.widgets.text("catalog", "riskbricks")` — catalog-portable |
| **Structured Logging** | JSON audit logs for request tracing and latency monitoring |
""")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 7: CI/CD
# ═══════════════════════════════════════════════════════════════════════════════
with tabs[6]:
    st.markdown('<p class="section-title">Databricks Asset Bundles</p>', unsafe_allow_html=True)
    st.code("""
# databricks.yml
bundle:
  name: riskbricks

targets:
  dev:      { mode: development, default: true }
  staging:  { mode: development }
  prod:     { mode: production }
""", language="yaml")

    st.markdown('<p class="section-title">GitHub Actions — 4-Gate Pipeline</p>', unsafe_allow_html=True)
    st.code("""
┌──────────┐    ┌──────────────┐    ┌───────────────────┐    ┌──────────────────┐
│  Gate 1  │───▶│    Gate 2    │───▶│      Gate 3       │───▶│     Gate 4       │
│  Lint    │    │  Unit Tests  │    │ Integration Tests │    │ Deploy to Prod   │
│          │    │              │    │  (on Databricks)  │    │ (manual approve) │
│ black    │    │ pytest       │    │ notebook job run  │    │ bundle deploy    │
│ isort    │    │ config tests │    │ on main only      │    │ --target prod    │
│ flake8   │    │ guardrails   │    │                   │    │                  │
└──────────┘    └──────────────┘    └───────────────────┘    └──────────────────┘
""", language=None)

    st.markdown('<p class="section-title">Agent Deployment Pipeline</p>', unsafe_allow_html=True)
    st.code("""
01_register_uc_tools → 02_create_agent → 03_deploy_agent
         │                    │                  │
    11 SQL functions     Log model with      Deploy to
    in agent_tools       MLflow + UC         Model Serving
    schema               Registry            Endpoint
""", language=None)

    st.markdown('<p class="section-title">Scheduled Jobs</p>', unsafe_allow_html=True)
    st.markdown("""
| Job | Schedule | What It Does |
|-----|----------|--------------|
| `daily_data_refresh` | Daily 6 AM ET | Stock prices → technical indicators → portfolio risk metrics |
| `daily_gdelt_refresh` | Daily 7 AM ET | GDELT events → geopolitical features |
| `ml_predictions_refresh` | Daily 8 AM ET | RSS + FRED + features → ML predictions |
| `rebuild_derived_tables` | Daily 9 AM ET | Rebuild gold analytics tables |
| `data_quality_checks` | Daily 10 AM ET | Validate row counts, freshness, schema integrity |
""")


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("RiskBricks · Built on Databricks Lakehouse · Presented at Databricks Summit 2026")
