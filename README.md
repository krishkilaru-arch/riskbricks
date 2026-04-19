# RiskBricks — Portfolio Risk Analytics on Databricks

Full-stack portfolio risk analytics platform powered by Databricks Lakehouse, AI agents, and ML models.

**One question**: *"What is the risk in my portfolio and what should I do about it?"*

---

## Architecture at a Glance

```
4 Sources → 5 Bronze → 8 Silver → 12 Gold → 11 UC Functions → 6 AI Agents → Streamlit App
```

| Layer | Tables | Purpose |
|-------|--------|---------|
| Bronze | 5 | Raw ingestion (Yahoo Finance, FRED, RSS, GDELT, static config) |
| Silver | 8 | Cleaned + enriched (prices, features, sentiment, ML training) |
| Gold | 12 | Business-ready (risk metrics, forecasts, signals, predictions) |
| Pipelines | 6 | SDP streaming tables |
| Agents | 9 | Agent inference tables |
| **Total** | **40** | |

**Catalog**: `riskbricks` (Unity Catalog)
**App**: `riskbricks-app` (Streamlit, 5 pages)
**Endpoint**: `riskbricks-supervisor-agent` (Llama 3.3 70B)

---

## 6 AI Sub-Agents

| Sub-Agent | Purpose | UC Functions |
|-----------|---------|-------------|
| **Risk Agent** | VaR, stress tests, volatility, beta | `get_portfolio_risk_metrics`, `get_stress_test_results` |
| **Price Target Agent** | Price predictions at 1d/5d/15d with confidence bands | `get_stock_forecast` |
| **ML Direction Agent** | Ensemble UP/DOWN predictions with model confidence | `get_ml_stock_forecast`, `get_ml_market_overview` |
| **Factor Agent** | Fama-French betas + sector allocation | `get_factor_exposures`, `get_sector_exposures` |
| **Decision Agent** | Buy/Hold/Sell signals + macro context | `get_decision_signal`, `get_macro_context` |
| **News Agent** | News headlines + portfolio holdings detail | `get_news_context`, `get_portfolio_holdings` |

---

## Data Sources

| Source | API | Bronze Table | What |
|--------|-----|-------------|------|
| Yahoo Finance | `yfinance` | `stock_prices_bronze` (37K) | Daily OHLCV for 432 stocks |
| FRED | CSV API | `fred_macro_indicators` (162) | VIX, yields, oil, USD |
| Yahoo/Google RSS | `feedparser` | `news_rss_all` (76K) | Stock-specific news headlines |
| GDELT | ZIP/TSV | `historical_news_gdelt` (18.5M) | Global geopolitical events |
| Static config | Python | `portfolio_holdings_bronze` (430) | 3 managers, 139 positions |

---

## Project Structure

```
riskbricks/
├── notebooks/
│   ├── agents/           # AI agent lifecycle (4 files)
│   ├── gold/             # Gold layer compute (5 files)
│   ├── ingestion/        # Data ingestion + features (7 files)
│   ├── jobs/             # Daily scheduled jobs (2 files)
│   ├── pipelines/        # SDP streaming pipelines (2 files)
│   └── training/         # ML model training (1 file)
├── app/                  # Streamlit app (5 pages)
├── jobs/                 # Databricks job YAML configs
├── config/               # Agent configs
├── data/                 # Static portfolio data
└── DATA_ARCHITECTURE.md  # Full architecture documentation
```

**21 active notebooks** across 7 folders. All use `catalog` widget for portability.

---

## Quick Start

```bash
# 1. Run daily data refresh (stock prices → portfolio risk metrics)
# 2. Run ML data ingestion (RSS + FRED + features → ML predictions)
# 3. Run GDELT refresh (geopolitical events)
# 4. Deploy agents (register UC tools → create agent → deploy endpoint)
```

### Suggested Run Order

1. `jobs/daily_data_refresh` — stock prices first (everything depends on this)
2. `ingestion/ml_data_ingestion` — RSS + FRED + technical indicators + ML predictions
3. `jobs/daily_gdelt_refresh` — GDELT events (independent of 1-2)
4. `training/train_register_ensemble_model` — weekly model retraining (only when needed)

---

## ML Model

**Name**: `riskbricks.models.stock_forecast_ensemble` v1
**Algorithm**: Soft-vote ensemble (LightGBM + RandomForest + GradientBoosting)
**Accuracy**: 70.3% (walk-forward cross-validation)
**Features**: 31 features from 6 sources (prices, technical, sector, macro, news, GDELT)

---

## Portability

All notebooks use `dbutils.widgets.text("catalog", "riskbricks")` — change this one parameter to run on any workspace/catalog. No hardcoded paths, cluster IDs, or usernames.

---

## Future Work

Parked features that may be revisited:
- **RAG Knowledge Base** — see `notebooks/future_work/RAG_KNOWLEDGE_BASE.md`
- **Alt Signals & SEC Fundamentals** — see `notebooks/future_work/ALT_SIGNALS_AND_SEC_FUNDAMENTALS.md`

---

## License

See [LICENSE](LICENSE).
