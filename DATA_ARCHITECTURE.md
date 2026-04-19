# RiskBricks Data Architecture

## Overview

RiskBricks is a portfolio risk analytics platform on Databricks Lakehouse. It answers one question: **"What is the risk in my portfolio and what should I do about it?"**

To answer that, it collects stock prices, economic indicators, news sentiment, and geopolitical events — then transforms everything through Bronze → Silver → Gold layers into portfolio risk metrics, ML-powered stock forecasts, and AI agent responses.

**Catalog**: `riskbricks` (Unity Catalog)
**Schemas**: `bronze` · `silver` · `gold` · `pipelines` · `agents` · `agent_tools` · `models`
**Total**: 40 tables · 11 UC functions · 2 ML models · 1 serving endpoint · 1 Streamlit app

---

## Master Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL DATA SOURCES                               │
├──────────────┬──────────────┬──────────────┬──────────────┬────────────────┤
│  Yahoo Finance  │   FRED (Fed)   │   RSS Feeds    │     GDELT      │
│  Stock Prices   │   Macro Data   │ Yahoo + Google  │  Global Events │
│   (yfinance)    │   (CSV API)    │  (feedparser)   │  (ZIP files)   │
└────────┬────────┴───────┬────────┴───────┬─────────┴───────┬────────┘
         │                │                │                 │
         ▼                ▼                ▼                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Ingestion)                                                │
│  stock_prices_bronze · fred_macro_indicators · news_rss_all                  │
│  historical_news_gdelt · portfolio_holdings_bronze                       │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleaned + Enriched)                                           │
│  stock_prices · technical_indicators · sector_features · market_breadth      │
│  news_ai_sentiment · forecast_features_daily · macro_indicators              │
│  ml_training_features                                                      │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Business-Ready)                                                 │
│                                                                              │
│  PORTFOLIO          FORECASTS           RISK & ML                            │
│  ─────────          ─────────           ─────────                            │
│  company_universe   stock_forecasts     risk_factor_exposures                │
│  portfolio_holdings decision_signals    macro_indicators_daily               │
│  portfolio_managers ml_stock_predict.   ml_prediction_features               │
│  portfolio_risk_m.  sector_exposures                                         │
│  stress_test_res.                                                            │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SERVING LAYER                                                               │
│  AI Agent (6 sub-agents) → 11 UC Functions → Streamlit App (5 pages)         │
│  Endpoint: riskbricks-supervisor-agent  ·  LLM: Llama 3.3 70B               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## External APIs at a Glance

| What We Fetch | API / URL | Auth | Format | Notebook |
|---------------|-----------|------|--------|----------|
| Stock prices (OHLCV) | Yahoo Finance via `yfinance` | None | JSON → DataFrame | `notebooks/jobs/daily_data_refresh` |
| Macro indicators (VIX, yields, oil) | FRED `fred.stlouisfed.org/graph/fredgraph.csv` | None | CSV | `notebooks/ingestion/ml_data_ingestion` |
| News headlines | Yahoo RSS + Google News RSS | None | XML (RSS 2.0) | `notebooks/ingestion/ml_data_ingestion` |
| Global events & tone | GDELT `data.gdeltproject.org/events/` | None | ZIP → TSV | `notebooks/jobs/daily_gdelt_refresh` |

---

## A. Stock Market Data

**What**: Daily OHLCV (Open, High, Low, Close, Volume) for 432 Fortune 500 stocks
**Where from**: Yahoo Finance (`yfinance.download()`)
**Why**: Core pricing data for portfolio valuation, risk metrics, technical analysis, and ML features

### Bronze: `riskbricks.bronze.stock_prices_bronze` (~37K rows)

| Column | Type | Meaning |
|--------|------|---------|
| `symbol` | STRING | Ticker (AAPL, MSFT, etc.) |
| `date` | DATE | Trading date |
| `open` | DOUBLE | Opening price |
| `high` | DOUBLE | Day's high |
| `low` | DOUBLE | Day's low |
| `close` | DOUBLE | Closing price |
| `volume` | BIGINT | Shares traded |
| `adj_close` | DOUBLE | Split/dividend-adjusted close |
| `dividends` | DOUBLE | Dividend paid |
| `stock_splits` | DOUBLE | Split ratio (e.g., 2.0 = 2:1) |
| `capital_gains` | DOUBLE | Capital gains distribution |
| `price` | DOUBLE | Alias for close |
| `ingestion_timestamp` | TIMESTAMP | When ingested |

### Silver: `riskbricks.silver.stock_prices` (~1.1M rows)
Inherits all bronze columns plus:

| Column | Type | Meaning |
|--------|------|---------|
| `price_change_pct` | DOUBLE | Day-over-day % change |
| `is_anomaly` | BOOLEAN | True if change > 20% (likely data error) |
| `quality_score` | DOUBLE | 1.0 = valid close + volume, 0.5 = suspect |
| `validated_at` | TIMESTAMP | When validation ran |

### What it feeds (downstream)

```
bronze.stock_prices_bronze
    │
    ▼ validate + enrich
silver.stock_prices ──┬──▶ silver.technical_indicators (RSI, MACD, Bollinger)
                      ├──▶ silver.sector_features (sector momentum, breadth)
                      ├──▶ silver.market_breadth (advance/decline ratio)
                      ├──▶ silver.forecast_features_daily (29K feature rows)
                      │
                      ▼ join with portfolio config
                gold.company_universe (432 master symbols)
                      ├──▶ gold.portfolio_holdings (139 positions)
                      │       ├──▶ gold.portfolio_risk_metrics (VaR, beta)
                      │       ├──▶ gold.stress_test_results (4 scenarios)
                      │       └──▶ gold.sector_exposures
                      └──▶ gold.risk_factor_exposures (Fama-French betas)
```

### Key Gold Table: `riskbricks.gold.portfolio_risk_metrics` (3 rows)

| Column | Type | Meaning |
|--------|------|---------|
| `portfolio_id` | STRING | Portfolio identifier |
| `manager_name` | STRING | Sarah Russel, Rena Tang, or Mohit Arora |
| `risk_profile` | STRING | conservative / moderate / aggressive |
| `aum_usd` | DOUBLE | Assets under management ($) |
| `weighted_volatility_pct` | DOUBLE | Portfolio-weighted annualized volatility |
| `portfolio_beta` | DOUBLE | Beta vs S&P 500 |
| `var_1day_95_usd` | DOUBLE | 1-day Value-at-Risk, 95% confidence ($) |
| `var_10day_95_usd` | DOUBLE | 10-day VaR ($) |
| `computed_at` | TIMESTAMP | Last computation time |

**Notebook**: `notebooks/jobs/daily_data_refresh`

---

## B. Macroeconomic Indicators

**What**: 8 key economic indicators that affect equity markets
**Where from**: FRED (Federal Reserve Economic Data) — free CSV download
**Why**: VIX and yield spreads are features in the ML model; macro context for the AI agent

### What gets fetched

| FRED Series | We Call It | What It Measures |
|-------------|-----------|-----------------|
| `VIXCLS` | VIX | Market fear index (higher = more fear) |
| `T10Y2Y` | Yield Spread | 10Y minus 2Y Treasury (negative = recession signal) |
| `DFF` | Fed Funds Rate | Cost of overnight bank lending |
| `BAMLH0A0HYM2` | HY Credit Spread | Risk premium on junk bonds |
| `DGS10` | Treasury 10Y | 10-year government bond yield |
| `DGS2` | Treasury 2Y | 2-year government bond yield |
| `DTWEXBGS` | USD Index | Strength of the US dollar |
| `DCOILWTICO` | WTI Oil | Crude oil price per barrel |

### Bronze: `riskbricks.bronze.fred_macro_indicators` (~162 rows)

| Column | Type | Meaning |
|--------|------|---------|
| `indicator` | STRING | Indicator name (VIX, Yield_Spread_10Y2Y, etc.) |
| `date` | DATE | Observation date |
| `value` | DOUBLE | Indicator value |

### What it feeds

```
bronze.fred_macro_indicators
    ├──▶ silver.ml_training_features (feature: vix)
    ├──▶ gold.macro_indicators_daily (82 rows, served by get_macro_context)
    └──▶ ML ensemble model → gold.ml_stock_predictions
```

**Notebook**: `notebooks/ingestion/ml_data_ingestion` (Section 2)

---

## C. News & Events

Two separate news sources that serve different purposes:

### C1. RSS News Headlines

**What**: Stock-specific news from Yahoo Finance + Google News for 52 focus symbols
**Where from**: RSS feeds (`feedparser` library parses XML)
**Why**: AI sentiment scoring for ML features

**Feed URLs**:
- Yahoo: `https://feeds.finance.yahoo.com/rss/2.0/headline?s={SYMBOL}`
- Google: `https://news.google.com/rss/search?q={COMPANY}+stock`

#### Bronze: `riskbricks.bronze.news_rss_all` (~76K rows)

| Column | Type | Meaning |
|--------|------|---------|
| `symbol` | STRING | Stock ticker |
| `company_name` | STRING | Full company name |
| `sector` | STRING | GICS sector |
| `title` | STRING | Article headline |
| `content` | STRING | Article summary/snippet |
| `source` | STRING | yahoo_finance or google_news |
| `url` | STRING | Link to full article |
| `published_date` | DATE | Publication date |
| `doc_id` | STRING | Unique document ID |
| `ingestion_timestamp` | TIMESTAMP | When scraped |

#### What it feeds

```
bronze.news_rss_all
    ├──▶ silver.news_ai_sentiment (52 rows, one per symbol)
    │       └──▶ silver.ml_training_features (features: ai_sentiment, news_count)
    └──▶ SDP Pipeline: pipelines.news_rss_stream → news_sentiment_daily
```

**Notebook**: `notebooks/ingestion/ml_data_ingestion` (Section 1)

---

### C2. GDELT Global Events

**What**: Geopolitical events from 250+ news sources worldwide, filtered to our 432 stocks
**Where from**: GDELT Project — daily ZIP archives of tab-delimited event data
**Why**: Tone/sentiment signals for ML model; geopolitical risk context for agent

**URLs**:
- Events: `http://data.gdeltproject.org/events/{YYYYMMDD}.export.CSV.zip`

#### Bronze: `riskbricks.bronze.historical_news_gdelt` (~18.5M rows)

| Column | Type | Meaning |
|--------|------|---------|
| `event_id` | STRING | GDELT GlobalEventID |
| `event_date` | DATE | When the event occurred |
| `symbol` | STRING | Matched stock ticker |
| `company_name` | STRING | Company name |
| `actor1_name` | STRING | First actor (e.g., "United States") |
| `actor2_name` | STRING | Second actor (e.g., "China") |
| `goldstein_scale` | DOUBLE | Cooperation/conflict score (-10 to +10) |
| `num_mentions` | INT | How many times event was mentioned |
| `num_sources` | INT | Distinct news outlets covering it |
| `num_articles` | INT | Total articles about this event |
| `avg_tone` | DOUBLE | Average tone (-100 bad to +100 good) |
| `source_url` | STRING | Source article URL |
| `ingestion_timestamp` | TIMESTAMP | When ingested |


#### What it feeds

```
bronze.historical_news_gdelt
    ├──▶ SDP Pipeline: pipelines.news_gdelt_stream (4.7M) → sentiment → forecasts
    └──▶ silver.ml_training_features (features: gdelt_tone, gdelt_events)
```

**Notebook**: `notebooks/jobs/daily_gdelt_refresh`
**How it works**: Auto-detects MAX(event_date)+1 in existing table, downloads only new days, filters by company keywords, writes with `replaceWhere` for idempotency.

---

## D. Portfolio Configuration

**What**: 3 portfolio managers and their stock holdings
**Where from**: Static Python config file (`data/multi_manager_portfolios.py`)
**Why**: This is the "who owns what" that all risk calculations are built on

### Bronze: `riskbricks.bronze.portfolio_holdings_bronze` (430 rows)

| Column | Type | Meaning |
|--------|------|---------|
| `symbol` | STRING | Stock held |
| `sector` | STRING | GICS sector |
| `weight` | DOUBLE | Portfolio weight (0.0 to 1.0) |
| `value_usd` | DOUBLE | Position value in USD |
| `portfolio_id` | STRING | Which portfolio this belongs to |

### What it feeds

```
bronze.portfolio_holdings_bronze
    └──▶ gold.portfolio_holdings (139 active positions)
            ├──▶ gold.portfolio_risk_metrics (VaR, beta per manager)
            ├──▶ gold.stress_test_results (4 scenarios × 3 managers)
            └──▶ gold.sector_exposures (sector allocation %)
```

**Notebook**: `notebooks/ingestion/portfolio/ingest_setup_multi_manager_portfolios` (one-time setup)
Prices are refreshed daily by `daily_data_refresh`, which MERGEs current prices into holdings.

---

## ML Prediction Pipeline

This is the cross-cutting pipeline that **joins data from A, B, C1, C2** into a single ML feature vector, trains an ensemble model, and generates daily stock direction predictions.

### How it works

```
silver.stock_prices ──────────┐
silver.technical_indicators ──┤
silver.sector_features ───────┤
silver.market_breadth ────────┤  JOIN all
silver.news_ai_sentiment ─────┤  ────▶ silver.ml_training_features (408 rows, 31 features)
bronze.fred_macro_indicators ─┤              │
bronze.historical_news_gdelt ─┘              ▼
                                    ML Ensemble Model (LightGBM + RF + GB)
                                             │
                                             ▼
                                    gold.ml_stock_predictions (51 rows per run)
```

### Silver: `riskbricks.silver.ml_training_features` (408 rows)

Each row = one symbol on one date, with 31 features from 6 different sources:

| Feature | Source | What It Measures |
|---------|--------|-----------------|
| `return_5d` | Stock Prices | 5-day cumulative return |
| `return_20d` | Stock Prices | 20-day cumulative return |
| `volatility_20d` | Stock Prices | 20-day rolling volatility |
| `rsi_14` | Technical | Relative Strength Index (overbought/oversold) |
| `macd_hist` | Technical | MACD momentum signal |
| `bb_pct` | Technical | Position within Bollinger Bands (0=low, 1=high) |
| `vol_ratio` | Technical | Today's volume vs 20-day average |
| `gap_pct` | Technical | Overnight price gap |
| `close_position` | Technical | Close within day's range |
| `ai_sentiment` | RSS News | AI-classified sentiment (-1 to +1) |
| `news_count` | RSS News | Number of recent articles |
| `pos_articles` | RSS News | Positive article count |
| `neg_articles` | RSS News | Negative article count |
| `gdelt_tone` | GDELT | Average event tone for this stock |
| `gdelt_events` | GDELT | Number of GDELT events mentioning this stock |
| `sector_momentum_5d` | Sector | Sector-level 5-day momentum |
| `sector_breadth` | Sector | % of sector stocks going up |
| `advance_ratio` | Market | Market-wide advance/decline ratio |
| `pct_above_ma20` | Market | % of universe above 20-day moving average |
| `vix` | FRED | CBOE Volatility Index |
| `days_to_earnings` | Earnings | Days until next earnings report |
| `is_monday` | Calendar | Monday indicator (behavioral anomaly) |
| `actual_direction` | **Label** | Actual next-day direction (UP/DOWN) |

### ML Model

**Name**: `riskbricks.models.stock_forecast_ensemble` v1
**Algorithm**: Soft-vote ensemble of LightGBM + RandomForest + GradientBoosting
**Accuracy**: 70.3% (walk-forward cross-validation)
**Training notebook**: `notebooks/training/train_register_ensemble_model`
**Prediction notebook**: `notebooks/ingestion/ml_data_ingestion` (Section 7)

> **Important**: The ML feature table does NOT duplicate data — it JOINS existing silver/bronze tables into one ML-ready row per symbol. The raw data stays where it is; the feature table is a materialized view for model training.

---

## SDP Pipeline: News to Forecasts

**Type**: Lakeflow Spark Declarative Pipeline (serverless, Photon)
**Trigger**: Manual (run on-demand)
**Notebook**: `notebooks/pipelines/news_to_forecasts_pipeline`

This streaming pipeline processes news (RSS + GDELT) into real-time forecasts and buy/sell signals.

### Pipeline Flow

```
bronze.historical_news_gdelt ──▶ pipelines.news_gdelt_stream (4.7M rows)
                                        │
bronze.news_rss_all ──────────▶ pipelines.news_rss_stream (60K rows)
                                        │
                                        ▼
                            pipelines.news_sentiment_daily (38K rows)
                                        │
                                        ▼
                            pipelines.news_forecast_features (1.1M rows)
                                        │
                              ┌─────────┴──────────┐
                              ▼                    ▼
              pipelines.decision_signals_live   pipelines.stock_forecasts_live
                      (836 signals)                  (832 forecasts)
```

### Pipeline Tables (in `riskbricks.pipelines`)

| Table | Rows | What It Contains |
|-------|------|-----------------|
| `news_gdelt_stream` | 4.7M | Streaming view of GDELT events |
| `news_rss_stream` | 60K | Streaming view of RSS news |
| `news_sentiment_daily` | 38K | Daily aggregated sentiment per symbol |
| `news_forecast_features` | 1.1M | Combined news features for forecasting |
| `decision_signals_live` | 836 | Live BUY / HOLD / SELL signals |
| `stock_forecasts_live` | 832 | Live price direction forecasts |

---

## Agent Architecture

The AI agent answers portfolio questions like "What's my risk exposure?" or "Should I sell AAPL?" by calling UC functions that query gold tables.

**Endpoint**: `riskbricks-supervisor-agent` (always-on)
**Model**: `riskbricks.agents.riskbricks_agent` v16
**LLM**: Llama 3.3 70B (Databricks Foundation Model API)
**App**: `riskbricks-app` (Streamlit, 5 pages)

### 6 Sub-Agents → 11 UC Functions → 12 Gold → 8 Silver → 5 Bronze → 4 Sources

| Sub-Agent | Purpose | UC Functions | Gold Tables | Silver Tables | Bronze Tables | Source |
|-----------|---------|-------------|-------------|---------------|---------------|--------|
| Risk Agent | VaR, stress tests, volatility, beta | `get_portfolio_risk_metrics`, `get_stress_test_results` | portfolio_risk_metrics, stress_test_results, portfolio_holdings, portfolio_managers, company_universe | stock_prices | stock_prices_bronze, portfolio_holdings_bronze | Yahoo Finance, Static config |
| Price Target Agent | Price predictions with confidence bands (1d/5d/15d) | `get_stock_forecast` | stock_forecasts | forecast_features_daily | stock_prices_bronze | Yahoo Finance |
| ML Direction Agent | Ensemble UP/DOWN predictions with model confidence | `get_ml_stock_forecast`, `get_ml_market_overview` | ml_stock_predictions, ml_prediction_features | ml_training_features, forecast_features_daily, technical_indicators, sector_features, market_breadth, news_ai_sentiment | stock_prices_bronze, fred_macro_indicators, news_rss_all, historical_news_gdelt | Yahoo Finance, FRED, Yahoo/Google RSS, GDELT |
| Factor Agent | Fama-French betas + sector allocation | `get_factor_exposures`, `get_sector_exposures` | risk_factor_exposures, sector_exposures, portfolio_holdings, portfolio_managers, company_universe | stock_prices | stock_prices_bronze, portfolio_holdings_bronze | Yahoo Finance, Static config |
| Decision Agent | Buy/Hold/Sell signals + macro context | `get_decision_signal`, `get_macro_context` | decision_signals, macro_indicators_daily, stock_forecasts, portfolio_holdings, portfolio_managers, company_universe | forecast_features_daily, macro_indicators, stock_prices | stock_prices_bronze, fred_macro_indicators, portfolio_holdings_bronze | Yahoo Finance, FRED, Static config |
| News Agent | News headlines + portfolio holdings detail | `get_news_context`, `get_portfolio_holdings` | portfolio_holdings, portfolio_managers, company_universe | stock_prices | news_rss_all, portfolio_holdings_bronze, stock_prices_bronze | Yahoo/Google RSS, Yahoo Finance, Static config |

> **Notes**: `get_news_context` reads directly from `bronze.news_rss_all` (not a gold table). `company_universe` (432 rows) is a shared internal lookup — upstream dependency of `portfolio_holdings`, used by 24 notebooks, not directly served by any UC function.

---

## Notebooks Reference

All notebooks are in the `notebooks/` directory. All use `catalog` widget/variable for portability — no hardcoded paths.

**21 active files** across 7 folders:

### `jobs/` — Daily Scheduled Jobs

| Notebook | What It Does | Tables Written |
|----------|-------------|----------------|
| `daily_data_refresh` | Fetches stock prices → validates → computes portfolio risk metrics | bronze.stock_prices_bronze → silver.stock_prices → gold.company_universe, portfolio_holdings, portfolio_risk_metrics, stress_test_results, sector_exposures |
| `daily_gdelt_refresh` | Downloads incremental GDELT event data | bronze.historical_news_gdelt |

### `ingestion/` — Data Ingestion & Feature Engineering

| Notebook | What It Does | Tables Written |
|----------|-------------|----------------|
| `ml_data_ingestion` | Scrapes RSS + FRED, computes technical/sector/sentiment features, runs ML predictions | bronze.news_rss_all, fred_macro_indicators → silver.(6 tables) → gold.ml_prediction_features, ml_stock_predictions |
| `stocks/ingest_stocks_and_macros_data` | Initial stock + macro data ingestion | bronze.stock_prices_bronze, fred_macro_indicators |
| `stocks/bronze_to_gold_daily_stocks_macros` | Promotes validated macro indicators to gold | gold.macro_indicators_daily |
| `forecast/build_forecast_features_daily` | Builds forecast feature vectors from prices + GDELT | silver.forecast_features_daily |
| `gdelt/bronze_ingest_gdelt` | GDELT event ingestion (called by daily_gdelt_refresh) | bronze.historical_news_gdelt |
| `rss/bronze_ingest_rss_news` | RSS news ingestion (called by ml_data_ingestion) | bronze.news_rss_all |
| `portfolio/ingest_setup_multi_manager_portfolios` | One-time: creates 3 managers + 139 holdings | gold.company_universe, portfolio_holdings, portfolio_managers |

### `gold/` — Gold Layer Compute

| Notebook | What It Does | Tables Written |
|----------|-------------|----------------|
| `analytics/create_risk_analytics` | VaR, stress tests, sector exposures | gold.portfolio_risk_metrics, stress_test_results, sector_exposures |
| `analytics/build_portfolio_manager_outputs` | Decision signals, risk factor exposures | gold.decision_signals, risk_factor_exposures |
| `forecast/generate_stock_forecasts` | AI-powered price target generation | gold.stock_forecasts |
| `forecast/evaluate_stock_forecasts` | Forecast accuracy evaluation | (evaluation metrics) |
| `forecast/train_forecast_model` | Trains forecast model from features | (model artifact) |

### `training/` — ML Model Training

| Notebook | What It Does | Output |
|----------|-------------|--------|
| `train_register_ensemble_model` | Trains LightGBM + RF + GB ensemble, registers in Unity Catalog | `{catalog}.models.stock_forecast_ensemble` |

### `pipelines/` — Lakeflow Spark Declarative Pipelines

| Notebook | What It Does |
|----------|-------------|
| `news_to_forecasts_pipeline` | Streaming pipeline: news → sentiment → forecasts → signals |
| `ml_feature_pipeline` | ML feature assembly pipeline |

### `agents/` — AI Agent Lifecycle

| Notebook | What It Does |
|----------|-------------|
| `riskbricks_agent` | Agent logic: 6 sub-agents, supervisor routing, prompts |
| `01_register_uc_tools` | Registers 11 UC functions in `{catalog}.agent_tools` |
| `02_create_agent` | Creates agent model + logs to Unity Catalog |
| `03_deploy_agent` | Deploys agent to serving endpoint |

### Suggested Run Order (manual execution)

```
1. daily_data_refresh              ← stock prices first (everything depends on this)
2. ml_data_ingestion               ← RSS + FRED + technical indicators + ML predictions
3. daily_gdelt_refresh             ← GDELT events (independent of 1-2)
4. train_register_ensemble_model   ← weekly model retraining (only when needed)
```

---

## Future Work

Parked features that may be revisited:
- **RAG Knowledge Base** — see `notebooks/future_work/RAG_KNOWLEDGE_BASE.md`
- **Alt Signals & SEC Fundamentals** — see `notebooks/future_work/ALT_SIGNALS_AND_SEC_FUNDAMENTALS.md`

---

## Portability

All notebooks use `dbutils.widgets.text("catalog", "riskbricks")` — change this one parameter to run on any workspace/catalog. Symbols are loaded dynamically from `{catalog}.gold.company_universe`. No hardcoded paths, cluster IDs, or usernames.
