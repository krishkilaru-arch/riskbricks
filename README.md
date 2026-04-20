<p align="center">
  <h1 align="center">RiskBricks</h1>
  <p align="center"><strong>AI-Powered Portfolio Risk Analytics on Databricks Lakehouse</strong></p>
  <p align="center"><em>Presented at Databricks Summit 2026</em></p>
</p>

---

> **One question drives everything:** *"What is the risk in my portfolio — and what should I do about it?"*
>
> RiskBricks answers this by orchestrating 4 external data sources, a medallion lakehouse architecture,
> an ML ensemble model, and 6 specialized AI agents — all running on Databricks, governed by Unity Catalog.

---

## Table of Contents

1. [Platform at a Glance](#platform-at-a-glance)
2. [Architecture](#architecture)
3. [Databricks Features Used](#databricks-features-used)
4. [Data Sources & Ingestion](#data-sources--ingestion)
5. [Medallion Lakehouse Architecture](#medallion-lakehouse-architecture)
6. [ML Ensemble Model](#ml-ensemble-model)
7. [Multi-Agent AI System](#multi-agent-ai-system)
8. [Unity Catalog Functions](#unity-catalog-functions)
9. [Streamlit Application](#streamlit-application)
10. [Agent Evaluation Framework](#agent-evaluation-framework)
11. [CI/CD & Deployment](#cicd--deployment)
12. [Project Structure](#project-structure)
13. [Quick Start](#quick-start)
14. [Future Work](#future-work)

---

## Platform at a Glance

```
4 Sources → 5 Bronze → 8 Silver → 12 Gold → 11 UC Functions → 6 AI Agents → Streamlit App
```

| Metric | Count |
|--------|-------|
| Unity Catalog Tables | 40 (5 bronze, 8 silver, 12 gold, 6 pipeline, 9 agent) |
| UC Functions | 11 (SQL, registered in `agent_tools` schema) |
| ML Models | 2 (stock forecast ensemble, agent supervisor) |
| AI Sub-Agents | 6 (risk, price target, factor, decision, news, ML direction) |
| Serving Endpoint | 1 (`riskbricks-supervisor-agent`, Llama 3.3 70B) |
| Streamlit App Pages | 5 (AI Chat, Portfolio Mgmt, Risk Dashboard, Data Mgmt, ML Predictions) |
| Evaluation Questions | 220 across 11 categories |
| Overall Pass Rate | **93.2%** (v31, up from 75.9% baseline) |

**Catalog:** `riskbricks` &nbsp;|&nbsp; **Schemas:** `bronze` · `silver` · `gold` · `agent_tools` · `agents` · `models` · `pipelines`

---

## Architecture

### End-to-End System Architecture

```
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
│  ─────────────────────────  │  │                                                │
│  LightGBM + RF + GB        │  │  PORTFOLIO        FORECASTS       RISK & ML    │
│  Walk-Forward CV            │  │  ───────────      ─────────       ─────────    │
│  MLflow Experiment Tracking │  │  company_universe stock_forecasts factor_exp.  │
│  Unity Catalog Registry     │  │  portfolio_hold.  decision_sig.   macro_daily  │
│  70.3% Accuracy             │  │  portfolio_mgrs   ml_stock_pred.  ml_pred_feat │
│  Production Alias           │  │  portfolio_risk   sector_exp.                  │
│                             │  │  stress_test_res                               │
└─────────────┬───────────────┘  └──────────────────────┬─────────────────────────┘
              │                                         │
              └────────────┬────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  UNITY CATALOG FUNCTIONS  (11 SQL functions in `riskbricks.agent_tools`)        │
│  get_portfolio_risk_metrics · get_stress_test_results · get_stock_forecast      │
│  get_ml_stock_forecast · get_ml_market_overview · get_factor_exposures          │
│  get_sector_exposures · get_decision_signal · get_macro_context                 │
│  get_news_context · get_portfolio_holdings                                      │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  MULTI-AGENT AI SYSTEM  (LangGraph + Mosaic AI Agent Framework)                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │                    SUPERVISOR AGENT (Llama 3.3 70B)                     │    │
│  │         Routes queries → sub-agents → synthesizes responses            │    │
│  └────┬────────┬────────┬────────┬────────┬────────┬──────────────────────┘    │
│       │        │        │        │        │        │                            │
│       ▼        ▼        ▼        ▼        ▼        ▼                            │
│    ┌──────┐┌──────┐┌──────┐┌──────┐┌──────┐┌──────────┐                        │
│    │ Risk ││Price ││Factor││Decis.││ News ││ML Direct.│                        │
│    │Agent ││Target││Agent ││Agent ││Agent ││  Agent   │                        │
│    └──────┘└──────┘└──────┘└──────┘└──────┘└──────────┘                        │
│  Endpoint: riskbricks-supervisor-agent  ·  Model Serving (Serverless)           │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STREAMLIT APPLICATION  (Databricks Apps)                                       │
│  🤖 AI Agent Chat · 👥 Portfolio Mgmt · 📊 Risk Dashboard                      │
│  ⚙️  Data Management · 🎯 ML Predictions                                       │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Agent Routing Architecture

```
                          User: "Should I buy NVDA?"
                                    │
                                    ▼
                       ┌────────────────────────┐
                       │   Supervisor Agent      │
                       │   (Llama 3.3 70B)       │
                       │                        │
                       │  Parses intent →       │
                       │  Emits: {"next":        │
                       │   "decision_agent"}     │
                       └──────────┬─────────────┘
                                  │
                    ┌─────────────┼──────────────┐
                    ▼             ▼               ▼
            ┌──────────┐  ┌────────────┐  ┌────────────┐
            │ Decision  │  │ ML Direct. │  │   Price    │
            │  Agent    │  │   Agent    │  │  Target    │
            ├──────────┤  ├────────────┤  ├────────────┤
            │ UC Tool:  │  │ UC Tool:   │  │ UC Tool:   │
            │ get_      │  │ get_ml_    │  │ get_stock_ │
            │ decision_ │  │ stock_     │  │ forecast() │
            │ signal()  │  │ forecast() │  │            │
            ├──────────┤  ├────────────┤  ├────────────┤
            │ Gold:     │  │ Gold:      │  │ Gold:      │
            │ decision_ │  │ ml_stock_  │  │ stock_     │
            │ signals   │  │ predictions│  │ forecasts  │
            └──────────┘  └────────────┘  └────────────┘
                    │             │               │
                    └─────────────┼───────────────┘
                                  ▼
                    ┌────────────────────────┐
                    │  "The signal for NVDA   │
                    │   is **BUY** with a     │
                    │   score of 6.01..."      │
                    └────────────────────────┘
```

---

## Databricks Features Used

RiskBricks is designed as a showcase of the Databricks platform. Every major feature area is utilized:

| Feature | How RiskBricks Uses It |
|---------|------------------------|
| **Unity Catalog** | Single `riskbricks` catalog with 7 schemas; table-level governance, column comments, lineage tracking across all 40 tables |
| **Medallion Architecture** | Bronze (raw) → Silver (cleaned/enriched) → Gold (business-ready) with clear lineage |
| **Delta Lake** | All tables stored as Delta; MERGE for upserts, time travel for audit, Z-ORDER for query performance |
| **MLflow** | Experiment tracking, model logging with signatures, walk-forward CV metrics, artifact storage |
| **Unity Catalog Model Registry** | `riskbricks.models.stock_forecast_ensemble` with production alias, version lineage |
| **Model Serving** | `riskbricks-supervisor-agent` endpoint serving the multi-agent system on serverless compute |
| **Foundation Model APIs** | `databricks-meta-llama-3-3-70b-instruct` as the LLM backbone for all 6 sub-agents |
| **Mosaic AI Agent Framework** | `ChatAgent` wrapper, `mlflow.pyfunc.log_model` for agent deployment, agent evaluation |
| **LangGraph** | Supervisor → sub-agent routing graph with `StateGraph`, `create_react_agent`, conditional edges |
| **UC Functions (SQL)** | 11 registered functions in `agent_tools` schema — the tool interface between agents and data |
| **Databricks Apps** | Streamlit app (`riskbricks-app`) with 5 pages, served via `app.yaml` + Databricks Apps runtime |
| **Databricks Asset Bundles** | `databricks.yml` with dev/staging/prod targets for portable deployment |
| **GitHub Actions CI/CD** | 4-gate pipeline: lint → unit tests → integration tests → deploy (with manual prod approval) |
| **Serverless Compute** | Interactive notebooks + UC function execution on serverless |
| **Secrets Management** | `dbutils.secrets.get(scope="riskbricks", key="fred-api-key")` for API credentials |
| **Widgets** | `dbutils.widgets.text("catalog", "riskbricks")` — every notebook is catalog-portable |
| **Structured Logging** | JSON audit logs in agent serving for request tracing and latency monitoring |

---

## Data Sources & Ingestion

### External APIs

| Source | API | Volume | Refresh | Notebook |
|--------|-----|--------|---------|----------|
| **Yahoo Finance** | `yfinance` Python lib | 432 stocks × 90 days OHLCV | Daily | `jobs/daily_data_refresh` |
| **FRED** | `fred.stlouisfed.org` CSV | 8 macro indicators (VIX, yields, oil, USD) | Daily | `ingestion/ml_data_ingestion` |
| **RSS News** | Yahoo + Google News RSS | 52 symbols × ~1,500 articles | Daily | `ingestion/rss/bronze_ingest_rss_news` |
| **GDELT** | `data.gdeltproject.org` ZIP/TSV | 18.5M global events | Daily | `jobs/daily_gdelt_refresh` |
| **Portfolios** | Static Python config | 3 managers, 139 positions | On setup | `ingestion/portfolio/ingest_setup_multi_manager_portfolios` |

### Three Portfolio Managers

| Manager | Style | AUM | Positions | Beta |
|---------|-------|-----|-----------|------|
| **Sarah Russel** | Conservative | $90M | 45 | 0.8 |
| **Rena Tang** | Balanced | $72M | 46 | 0.9 |
| **Mohit Arora** | Aggressive | $180M | 48 | 1.0 |

---

## Medallion Lakehouse Architecture

### Bronze Layer — Raw Ingestion

| Table | Rows | Source | Key Columns |
|-------|------|--------|-------------|
| `stock_prices_bronze` | 37K | Yahoo Finance | symbol, date, open, high, low, close, volume |
| `fred_macro_indicators` | 162 | FRED | indicator, date, value |
| `news_rss_all` | 76K | RSS Feeds | symbol, title, published, source |
| `historical_news_gdelt` | 18.5M | GDELT | date, actor1, actor2, tone, goldstein_scale |
| `portfolio_holdings_bronze` | 430 | Static Config | manager_name, symbol, shares, weight |

### Silver Layer — Cleaned & Feature-Engineered

| Table | Rows | Derived From | Key Enrichments |
|-------|------|--------------|------------------|
| `stock_prices` | 1.1M | stock_prices_bronze | price_change_pct, quality_score, anomaly flags |
| `technical_indicators` | — | stock_prices | RSI-14, MACD histogram, Bollinger %B, volume ratio |
| `sector_features` | — | stock_prices | Sector relative momentum, breadth, dispersion |
| `market_breadth` | — | stock_prices | Advance/decline ratio, % above MA20, market dispersion |
| `news_ai_sentiment` | — | news_rss_all | AI sentiment score (-1 to +1), positive/negative counts |
| `forecast_features_daily` | 29K | All silver tables | 31-feature matrix for ML training |
| `macro_indicators` | — | fred_macro_indicators | Cleaned, forward-filled, with change metrics |
| `ml_training_features` | 408 | forecast_features_daily | Final training matrix with target labels |

### Gold Layer — Business-Ready Analytics

| Table | Rows | Purpose | Served By UC Function |
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
| `ml_stock_predictions` | 52 | UP/DOWN with confidence, model agreement | `get_ml_stock_forecast` |
| `ml_prediction_features` | — | Feature importance for explainability | `get_ml_market_overview` |

### Data Lineage

```
Yahoo Finance ──▶ stock_prices_bronze ──▶ stock_prices ──┬──▶ technical_indicators ──┐
                                                         ├──▶ sector_features ───────┤
                                                         ├──▶ market_breadth ─────────┤
                                                         │                            ▼
FRED ───────────▶ fred_macro_indicators ─────────────────┼──▶ forecast_features_daily
                                                         │           │
RSS Feeds ──────▶ news_rss_all ──▶ news_ai_sentiment ───┘           │
                                                                     ▼
GDELT ──────────▶ historical_news_gdelt ────────────────▶ ml_training_features
                                                                     │
                                                                     ▼
                                                              ML Ensemble Model
                                                                     │
          ┌──────────────────────┬──────────────────────┬────────────┘
          ▼                      ▼                      ▼
   stock_forecasts      decision_signals      ml_stock_predictions
          │                      │                      │
          └──────────────────────┴──────────────────────┘
                                 │
                          11 UC Functions
                                 │
                           6 AI Agents
                                 │
                          Streamlit App
```

---

## ML Ensemble Model

### Model Architecture

```
                    ml_training_features (408 samples × 31 features)
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             ┌────────────┐ ┌────────────┐ ┌────────────────┐
             │  LightGBM  │ │  Random    │ │  Gradient      │
             │            │ │  Forest    │ │  Boosting      │
             │ leaves=8   │ │ trees=100  │ │  trees=50      │
             │ lr=0.1     │ │ depth=5    │ │  depth=3       │
             │ n_est=50   │ │            │ │  lr=0.1        │
             └──────┬─────┘ └──────┬─────┘ └───────┬────────┘
                    │              │                │
                    └──────────────┼────────────────┘
                                   ▼
                          Soft Vote Ensemble
                        (avg probabilities)
                                   │
                                   ▼
                        ┌──────────────────┐
                        │  UP / DOWN       │
                        │  + confidence    │
                        │  + model agree.  │
                        └──────────────────┘
```

| Metric | Value |
|--------|-------|
| **Algorithm** | Soft-vote ensemble (LightGBM + RandomForest + GradientBoosting) |
| **Target** | Binary classification — next-day direction (UP / DOWN) |
| **Features** | 31 features from 6 data sources |
| **Validation** | Walk-forward cross-validation (train on days 1..N-1, predict day N) |
| **Overall Accuracy** | 70.3% |
| **High-Confidence Accuracy** | 76.7% (when confidence > 40%) |
| **Registered As** | `riskbricks.models.stock_forecast_ensemble` v1 with `@production` alias |
| **Top Feature** | `gap_pct` (overnight gap) — #1 by importance |

### Feature Groups (31 Features)

| Group | Features | Source |
|-------|----------|--------|
| **Price/Technical** (10) | return_5d, return_20d, volatility_20d, rsi_14, macd_hist, bb_pct, vol_ratio, avg_range_5, gap_pct, close_position | Yahoo Finance → Silver |
| **Sector** (4) | sector_rel_5d, sector_momentum_5d, sector_breadth, stock_vs_sector_1d | Sector features |
| **Market** (4) | market_return, advance_ratio, pct_above_ma20, market_dispersion | Market breadth |
| **Macro** (3) | vix, hy_spread, treasury_10y | FRED |
| **News/Sentiment** (4) | ai_sentiment, news_count, pos_articles, neg_articles | RSS → AI scoring |
| **Events** (2) | gdelt_tone, gdelt_events | GDELT |
| **Calendar** (4) | days_to_earnings, earnings_within_5d, day_of_week, month | Derived |

### MLflow Integration

- **Experiment**: `riskbricks_stock_forecast` — tracks all training runs
- **Logged Artifacts**: Model pickle, feature importance plot, confusion matrix, walk-forward results
- **Signature**: Inferred from training data — enforced at prediction time
- **Input Example**: Stored with model for serving validation
- **Production Alias**: `@production` tag in Unity Catalog Model Registry

---

## Multi-Agent AI System

### Supervisor + 6 Sub-Agents

Built with **LangGraph** (state graph with conditional routing) and **Mosaic AI Agent Framework** (`ChatAgent` for MLflow-compatible serving).

| Sub-Agent | Domain | UC Functions | Key Outputs |
|-----------|--------|--------------|-------------|
| **Risk Agent** | Portfolio risk analysis | `get_portfolio_risk_metrics`, `get_stress_test_results` | VaR (1d/10d, 95%), beta, volatility, stress scenarios |
| **Price Target Agent** | Stock price forecasting | `get_stock_forecast` | 1d/15d predictions with confidence bands |
| **ML Direction Agent** | Ensemble ML predictions | `get_ml_stock_forecast`, `get_ml_market_overview` | UP/DOWN direction, confidence, model agreement |
| **Factor Agent** | Factor & sector analysis | `get_factor_exposures`, `get_sector_exposures` | Fama-French betas, sector weight breakdowns |
| **Decision Agent** | Investment signals | `get_decision_signal`, `get_macro_context` | BUY/HOLD/SELL with conviction score, macro context |
| **News Agent** | Financial news & holdings | `get_news_context`, `get_portfolio_holdings` | Recent headlines, portfolio position detail |

### Key Technical Decisions

| Decision | Rationale |
|----------|----------|
| **Llama 3.3 70B** | Best open-source instruction-following model available on Databricks Foundation APIs; temp=0.1 for deterministic output |
| **LangGraph over LangChain** | State graph enables supervisor routing with re-entry prevention and recursion limits |
| **UC Functions as tools** | SQL functions execute on serverless compute; decouples data access from agent logic; governed by Unity Catalog |
| **Supervisor FINISH suppression** | Critical fix (v31): prevents the supervisor's LLM-generated summary from burying sub-agent responses |
| **Post-processing signal extraction** | Parses UC function CSV responses to extract BUY/HOLD/SELL signals and prepend to agent output |

### Production Guardrails

```python
MAX_INPUT_LENGTH = 2000          # Reject oversized prompts
MAX_GRAPH_RECURSION = 6          # Prevent infinite agent loops
BLOCKED_PATTERNS = [...]         # Prompt injection detection
SENSITIVE_OUTPUT_PATTERNS = [...] # Redact leaked API keys/secrets
```

- **Input validation**: Length cap + prompt injection detection
- **Output sanitization**: Regex-based redaction of sensitive content
- **Re-routing prevention**: Supervisor tracks which agents already responded
- **Structured audit logging**: JSON logs with request_id, latency, agent_route
- **Deduplication**: Sentence-level and content-fingerprint dedup to prevent repetitive LLM output

---

## Unity Catalog Functions

11 SQL functions registered in `riskbricks.agent_tools` serve as the bridge between AI agents and gold-layer data:

| Function | Query Pattern | Returns |
|----------|---------------|---------|
| `get_portfolio_risk_metrics(manager)` | SELECT from portfolio_risk_metrics | VaR, beta, volatility, AUM |
| `get_stress_test_results(manager)` | SELECT from stress_test_results | 4 scenarios with $ and % impact |
| `get_stock_forecast(symbol)` | SELECT from stock_forecasts | 1d/15d price, direction, confidence bands |
| `get_ml_stock_forecast(symbol)` | SELECT from ml_stock_predictions | UP/DOWN, confidence, model agreement |
| `get_ml_market_overview()` | Aggregates ml_stock_predictions | Market sentiment, sector breakdown |
| `get_factor_exposures(symbol)` | SELECT from risk_factor_exposures | Market, SMB, HML betas, alpha |
| `get_sector_exposures(manager)` | SELECT from sector_exposures | Sector weights with % breakdown |
| `get_decision_signal(symbol)` | SELECT from decision_signals | BUY/HOLD/SELL, score, expected return |
| `get_macro_context()` | SELECT from macro_indicators_daily | Fed Funds, CPI, GDP, VIX, S&P 500 |
| `get_news_context(symbol, sector)` | SELECT from news_rss_all | Recent headlines with sentiment |
| `get_portfolio_holdings(manager)` | JOIN holdings + managers | Position-level detail with weights |

---

## Streamlit Application

Deployed as a **Databricks App** (`riskbricks-app`) with 5 interactive pages:

| Page | Purpose | Key Features |
|------|---------|-------------|
| 🤖 **AI Agent Chat** | Conversational interface to the multi-agent system | Real-time streaming, chat history, agent routing visibility |
| 👥 **Portfolio Management** | View and manage 3 portfolio managers | Holdings breakdown, position weights, manager comparison |
| 📊 **Risk Dashboard** | Visual risk analytics | VaR charts, stress test heatmaps, beta/volatility trends |
| ⚙️ **Data Management** | Monitor data freshness and quality | Table row counts, last refresh timestamps, quality scores |
| 🎯 **ML Predictions** | Explore ML ensemble outputs | Direction predictions, confidence filters, feature importance |

**App Configuration** (`app.yaml`):
```yaml
command: ["bash", "start.sh"]
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: "sql-warehouse"
  - name: RISKBRICKS_AGENT_ENDPOINT
    valueFrom: "serving-endpoint"
```

---

## Agent Evaluation Framework

### Evaluation Suite

**Location**: `demo/agent_evaluation` notebook
**Methodology**: 220 questions across 11 categories, graded automatically with 5 quality checks per question.

| Check | What It Validates |
|-------|-------------------|
| `has_answer` | Response is non-empty (>10 chars) |
| `no_error` | No generic error messages |
| `has_keyword` | Contains at least 1 expected keyword from the question's keyword list |
| `clean_dollar` | No backslash-escaped `\$` signs (LaTeX formatting bug) |
| `has_table` | Response includes markdown table formatting |

**PASS criteria**: `has_answer` AND `no_error` AND `has_keyword`

### Question Distribution

| Category | Questions | Tests |
|----------|-----------|-------|
| portfolio_risk | 30 | VaR, stress tests, AUM, beta, volatility, risk profiles |
| price_target | 30 | Price predictions, confidence bands, direction outlook |
| decision_signal | 30 | BUY/HOLD/SELL signals, conviction scores, expected returns |
| factor | 20 | Fama-French exposures, market/SMB/HML betas |
| sector | 20 | Sector weights, diversification, allocation comparison |
| holdings | 20 | Portfolio positions, stock overlap, position weights |
| news | 15 | Headlines, breaking news, market events |
| macro | 15 | GDP, inflation, Fed Funds rate, economic outlook |
| ml_forecast | 15 | ML direction predictions, model confidence, market overview |
| cross_agent | 15 | Multi-agent queries requiring 2+ agents to collaborate |
| edge_case | 10 | Invalid inputs, unknown symbols, prompt injection attempts |

### Version History & Results

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

### Key Fixes That Drove Improvement

| Version | Fix | Impact |
|---------|-----|--------|
| **v24** | Added "CRITICAL — ALWAYS STATE THE DIRECTION" to ML_DIRECTION_PROMPT with required UP/DOWN keywords | ml_forecast: 60% → 93% |
| **v26** | Added `recommendation` text column to `get_decision_signal` UC function + few-shot examples in DECISION_PROMPT | ml_forecast: → 100%, cross_agent: 47% → 73% |
| **v29** | Excluded `delta-spark==3.4.0` from pip requirements (non-existent PyPI version was blocking deployment) | Deployment fix |
| **v31** | **Supervisor FINISH suppression** — when routing to FINISH, emit pure `{"next": "FINISH"}` instead of LLM summary text | **Overall: 75.5% → 93.2%** — the supervisor's summary was burying sub-agent responses |

### Root Cause: The Supervisor FINISH Bug (v31 Fix)

The single most impactful fix in the project. Before v31, the supervisor would emit its own LLM-generated summary when finishing:

```
Message[0]: [decision_agent]: | Symbol | Signal | ... | NVDA | BUY | 6.01 | ...   ← HAS SIGNAL ✅
Message[1]: The beta of 1.7 indicates NVDA is volatile. {"next": "FINISH"}        ← NO SIGNAL ❌
                                                                                     ↑ eval picks this
```

The evaluation harness (and any client) takes `messages[-1]` — which was the supervisor's lossy paraphrase, not the decision agent's actual data-driven response. The fix: suppress the supervisor's text content when routing to FINISH.

Results saved to: `riskbricks.gold.agent_eval_results` (Delta table with model_version tracking).

---

## CI/CD & Deployment

### Databricks Asset Bundles

```yaml
# databricks.yml
bundle:
  name: riskbricks

targets:
  dev:      { mode: development, default: true }
  staging:  { mode: development }
  prod:     { mode: production }
```

### GitHub Actions — 4-Gate Pipeline

```
┌──────────┐    ┌──────────────┐    ┌───────────────────┐    ┌──────────────────┐
│  Gate 1  │───▶│    Gate 2    │───▶│      Gate 3       │───▶│     Gate 4       │
│  Lint    │    │  Unit Tests  │    │ Integration Tests │    │ Deploy to Prod   │
│          │    │              │    │  (on Databricks)  │    │ (manual approve) │
│ black    │    │ pytest       │    │ notebook job run  │    │ bundle deploy    │
│ isort    │    │ config tests │    │ on main only      │    │ --target prod    │
│ flake8   │    │ guardrails   │    │                   │    │                  │
└──────────┘    └──────────────┘    └───────────────────┘    └──────────────────┘
```

### Agent Deployment Pipeline

```
01_register_uc_tools.py → 02_create_agent.py → 03_deploy_agent.py
         │                        │                      │
    11 SQL functions         Log model with          Deploy to
    in agent_tools           MLflow + UC             Model Serving
    schema                   Registry                Endpoint
```

### Scheduled Jobs

| Job | Schedule | What It Does |
|-----|----------|--------------|
| `daily_data_refresh` | Daily 6 AM ET | Stock prices → technical indicators → portfolio risk metrics |
| `daily_gdelt_refresh` | Daily 7 AM ET | GDELT events → geopolitical features |
| `ml_predictions_refresh` | Daily 8 AM ET | RSS + FRED + features → ML predictions |
| `rebuild_derived_tables` | Daily 9 AM ET | Rebuild gold analytics tables |
| `data_quality_checks` | Daily 10 AM ET | Validate row counts, freshness, schema integrity |

---

## Project Structure

```
riskbricks/
├── README.md                              ← You are here
├── databricks.yml                         ← Databricks Asset Bundles config
├── requirements.txt                       ← Python dependencies
├── LICENSE
│
├── notebooks/
│   ├── agents/                            ← AI Agent Lifecycle
│   │   ├── riskbricks_agent.py            ← Core agent code (supervisor + 6 sub-agents)
│   │   ├── 01_register_uc_tools           ← Register 11 UC functions
│   │   ├── 02_create_agent                ← Log agent model to MLflow + UC Registry
│   │   └── 03_deploy_agent                ← Deploy to Model Serving endpoint
│   │
│   ├── ingestion/                         ← Data Ingestion
│   │   ├── stocks/
│   │   │   ├── ingest_stocks_and_macros_data
│   │   │   └── bronze_to_gold_daily_stocks_macros
│   │   ├── rss/bronze_ingest_rss_news
│   │   ├── gdelt/bronze_ingest_gdelt
│   │   ├── portfolio/ingest_setup_multi_manager_portfolios
│   │   ├── forecast/build_forecast_features_daily
│   │   └── ml_data_ingestion
│   │
│   ├── gold/                              ← Gold Layer Compute
│   │   ├── analytics/
│   │   │   ├── create_risk_analytics
│   │   │   └── build_portfolio_manager_outputs
│   │   └── forecast/
│   │       ├── train_forecast_model
│   │       ├── generate_stock_forecasts
│   │       └── evaluate_stock_forecasts
│   │
│   ├── training/                          ← ML Model Training
│   │   └── train_register_ensemble_model
│   │
│   ├── jobs/                              ← Scheduled Job Notebooks
│   │   ├── daily_data_refresh
│   │   ├── daily_gdelt_refresh
│   │   ├── ml_predictions_refresh
│   │   ├── rebuild_derived_tables
│   │   ├── data_quality_checks
│   │   └── news_to_forecasts_refresh.py
│   │
│   └── tests/                             ← Integration Tests
│       └── test_supervisor_agent
│
├── app/                                   ← Streamlit Application
│   ├── app.yaml                           ← Databricks App config
│   ├── Home.py                            ← Main entry point
│   ├── db_utils.py                        ← Database connection utilities
│   ├── start.sh                           ← Startup script
│   ├── requirements.txt                   ← App-specific dependencies
│   └── pages/
│       ├── 1_🤖_AI_Agent_Chat.py
│       ├── 2_👥_Portfolio_Management.py
│       ├── 3_📊_Risk_Dashboard.py
│       ├── 4_⚙️_Data_Management.py
│       └── 5_🎯_ML_Predictions.py
│
├── config/                                ← Centralized Configuration
│   ├── riskbricks_config.py               ← Auto-detecting config singleton
│   ├── constants.py                       ← Feature lists, model params
│   └── agents/                            ← Agent-specific configs
│
├── demo/                                  ← Evaluation & Demo
│   └── agent_evaluation                   ← 220-question eval suite
│
├── data/                                  ← Static Data Files
│
├── tests/                                 ← Unit Tests
│
├── .github/workflows/ci.yml               ← GitHub Actions CI/CD
│
└── _archived/                             ← Legacy notebooks (preserved)
```

**21 active notebooks** across 7 folders. All notebooks use `dbutils.widgets.text("catalog", "riskbricks")` for workspace portability.

---

## Quick Start

### 1. Initial Setup
```bash
# Clone and configure
git clone <repo-url>
cd riskbricks
databricks bundle deploy --target dev
```

### 2. Data Pipeline (run in order)
```
1. notebooks/jobs/daily_data_refresh          ← Stock prices, portfolio risk
2. notebooks/ingestion/ml_data_ingestion      ← RSS, FRED, features, ML predictions
3. notebooks/jobs/daily_gdelt_refresh         ← GDELT geopolitical events
4. notebooks/training/train_register_ensemble_model  ← Train ML model (weekly)
```

### 3. Agent Deployment
```
1. notebooks/agents/01_register_uc_tools      ← 11 UC functions
2. notebooks/agents/02_create_agent           ← Log model to UC Registry
3. notebooks/agents/03_deploy_agent           ← Deploy to Model Serving
```

### 4. Evaluation
```
demo/agent_evaluation                         ← Run 220-question eval suite
```

### Portability

Every notebook reads its catalog from a widget:
```python
dbutils.widgets.text("catalog", "riskbricks")
CATALOG = dbutils.widgets.get("catalog")
```
Change this one parameter to run the entire platform on any workspace/catalog.

---

## Future Work

| Feature | Status | Description |
|---------|--------|-------------|
| RAG Knowledge Base | Planned | Vector search over SEC filings, earnings transcripts |
| Alt Signals & SEC Fundamentals | Planned | Options flow, insider trading, 10-K/10-Q parsing |
| Real-time Streaming | Planned | Kafka/Kinesis ingestion for live price feeds |
| A/B Testing Framework | Planned | Compare agent versions on live traffic |
| Fine-tuned LLM | Planned | Domain-specific fine-tuning on financial Q&A |

---

## License

See [LICENSE](LICENSE).
