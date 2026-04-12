# RiskBricks: End-to-End AI Portfolio Risk Platform on Databricks

RiskBricks is a full-stack Databricks solution for multi-manager portfolio risk analytics. It combines ingestion pipelines, lakehouse modeling, agentic reasoning, and a Databricks App UI to deliver explainable risk insights for investment teams.

> **Note for reviewers:** This repository is a shorter, proposal-focused version of the actual product, intentionally trimmed for the Databricks proposal process. Additional enhancements and refinements are in progress ahead of summit presentation.

## Proposal PDFs

- [Databricks Summit 2026 Proposal](./Databricks%20Summit%202026%20Proposal.pdf)
- [RiskBricks Detailed Document](./RiskBricks_Detailed_Document.pdf)

## What This Project Solves

Portfolio risk teams often work across fragmented reports, delayed market signals, and manual analysis. RiskBricks provides:

- continuous ingestion of market, macro, and news signals
- standardized Bronze/Silver/Gold data modeling
- AI agents that call governed Unity Catalog tools
- a UI for portfolio managers to explore risk, forecast, and decisions
- reproducible deployment through Databricks Asset Bundles

## Multi-Manager Context

RiskBricks tracks three representative managers with different mandates:

| Manager | Risk Profile | Strategy | Target Return | Holdings | AUM |
|---------|--------------|----------|---------------|----------|-----|
| Sarah Russel | Conservative | Capital Preservation | 7% | 35 | $50M |
| Rena Tang | Balanced | Growth & Income | 11% | 60 | $75M |
| Mohit Arora | Aggressive | High-Growth Tech | 18% | 45 | $100M |

## End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL DATA SOURCES                            │
│  Yahoo Finance │ FRED (Macro) │ RSS/News │ GDELT Events │ SEC Filings  │
└───────┬─────────────┬──────────────┬────────────┬──────────────┬────────┘
        │             │              │            │              │
        ▼             ▼              ▼            ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE (notebooks/00_bronze/)                                          │
│  Raw ingestion → Delta tables in Unity Catalog                          │
│  ┌──────────────┐ ┌───────────────────┐ ┌────────┐ ┌─────────────────┐ │
│  │stock_prices  │ │macro_indicators   │ │news_rss│ │historical_news  │ │
│  │_bronze       │ │_bronze            │ │_all    │ │_gdelt           │ │
│  └──────────────┘ └───────────────────┘ └────────┘ └─────────────────┘ │
│  portfolio_holdings_bronze │ rag_corpus                                  │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER (notebooks/02_silver/)                                          │
│  Validation, dedup, quality scoring                                     │
│  ┌──────────────┐ ┌───────────────────┐ ┌──────────────┐ ┌───────────┐ │
│  │stock_prices  │ │macro_indicators   │ │forecast_     │ │rag_       │ │
│  │(1.1M rows,   │ │(6 series,         │ │features_daily│ │documents  │ │
│  │ 414 symbols) │ │ 10yr history)     │ │              │ │           │ │
│  └──────────────┘ └───────────────────┘ └──────────────┘ └───────────┘ │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD (notebooks/03_gold/)                                              │
│  Analytics, risk metrics, forecasts, RAG assets                         │
│                                                                         │
│  Portfolio Layer:          Risk Layer:           AI/RAG Layer:           │
│  ┌──────────────────┐     ┌──────────────────┐  ┌───────────────────┐  │
│  │portfolio_managers │     │portfolio_risk_   │  │rag_corpus         │  │
│  │(3 managers)       │     │metrics (VaR,β)   │  │rag_evidence_log   │  │
│  │portfolio_holdings │     │stress_test_      │  │rag_sector_insights│  │
│  │(139 positions)    │     │results           │  │news_impact_history│  │
│  │company_universe   │     │risk_factor_      │  │rag_news_timeline  │  │
│  └──────────────────┘     │exposures         │  └───────────────────┘  │
│                            └──────────────────┘                         │
│  Forecast Layer:           Decision Layer:                              │
│  ┌──────────────────┐     ┌──────────────────┐                         │
│  │stock_forecasts   │     │decision_signals  │                         │
│  │forecast_eval     │     │accuracy_scoreboard│                        │
│  │forecast_daily    │     │attribution_summary│                        │
│  └──────────────────┘     └──────────────────┘                         │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  AGENT LAYER (notebooks/04_agents/ + mosaic_agents/)                    │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────┐          │
│  │            SUPERVISOR AGENT (Llama 3.3 70B)              │          │
│  │         Routes requests → specialized tool agents         │          │
│  └──────┬──────┬──────┬──────┬──────┬──────┬───────┘          │
│         │      │      │      │      │      │                   │
│         ▼      ▼      ▼      ▼      ▼      ▼                   │
│   Retrieval Forecast Risk  Factor  News  Decision              │
│   Agent    Agent    Agent  Agent   Agent Agent                  │
│                                                                 │
│  Unity Catalog Functions (riskbricks.agent_tools.*)             │
│  get_risk_metrics │ get_portfolio_holdings │ compare_managers    │
│  get_stress_tests │ get_sector_exposures  │ query_rag           │
│  get_historical_news_impact │ predict_portfolio_news_impact     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SERVING LAYER (app/)                                                   │
│  Databricks App (Streamlit) → 4 pages                                   │
│                                                                         │
│  ┌────────────┐ ┌────────────────┐ ┌─────────────┐ ┌───────────────┐  │
│  │ AI Agent   │ │ Portfolio      │ │ Risk        │ │ Data          │  │
│  │   Chat     │ │  Management    │ │  Dashboard   │ │  Management   │  │
│  └────────────┘ └────────────────┘ └─────────────┘ └───────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  DEPLOYMENT (jobs1/ + databricks.yml)                                   │
│  15 scheduled workflows │ Databricks Asset Bundles │ MLflow tracking    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Core Components

- `notebooks/00_bronze`: ingestion of stocks, macro, RSS, news, GDELT, and RAG source data
- `notebooks/02_silver`: validation, normalization, and data quality pipelines
- `notebooks/03_gold`: risk analytics, forecast models, stress testing, and vector/RAG outputs
- `notebooks/04_agents` and `notebooks/agents`: multi-agent workflows and supervisor orchestration
- `notebooks/mosaic_agents`: deployment patterns for Mosaic AI / managed agent workflows
- `jobs1`: scheduled jobs for ingestion, forecasting, risk, decisioning, and evaluation
- `config/agents` and `config/mosaic_agents`: endpoint/tool configuration
- `app`: Databricks App UI (Streamlit)
- `databricks.yml`: bundle deployment config for jobs and app resource

## Databricks App UI

The project includes a deployable Databricks App that serves as the product UI.

- App source: `app/`
- App config: `app/app.yaml`
- Start command: `app/start.sh`
- Bundle app resource: `resources.apps.riskbricks_ui` in `databricks.yml`

UI pages include:
- AI Agent Chat
- Portfolio Management
- Risk Dashboard
- Data Management

## Agent System Design

RiskBricks implements a practical multi-agent pattern:

- Supervisor agent routes requests to specialized tools/agents
- Retrieval agent handles contextual lookups and evidence gathering
- Forecast agent generates and evaluates market projections
- Risk agent computes VaR, stress impacts, and exposure summaries
- Factor/news agents enrich portfolio context
- Decision and output agents produce explainable recommendations

The design prioritizes:
- transparency (traceable tool calls and outputs)
- governance (Unity Catalog functions and permissions)
- modularity (agent specialization with explicit boundaries)
- production migration path to managed Databricks agent services

## Data and Analytics Scope

Supported analytical capabilities include:

- real-time and scheduled market data ingestion
- macro sensitivity and scenario stress testing
- factor exposure analysis (value, momentum, quality, concentration)
- manager-level and cross-manager comparison metrics
- narrative risk summaries suitable for business and compliance stakeholders

Data sources include:
- FRED (macro indicators)
- market data feeds and symbol universes
- RSS/news streams and GDELT-style event signals
- synthetic/sample datasets for reproducible demos

## Deployment

### Requirements

- Databricks workspace with Unity Catalog
- Databricks CLI configured
- Python 3.8+

### Bundle Deployment

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

Run scheduled resources and workflows through bundle-managed jobs after deploy.

### App Deployment (direct)

```bash
databricks apps deploy riskbricks-app --source-code-path app/
```

## Proposal/Demo Narrative

For proposal or summit demos, the recommended flow is:

1. Show ingestion and quality checkpoints from Bronze to Silver
2. Show Gold risk outputs and forecast artifacts
3. Ask the supervisor agent portfolio questions across the three managers
4. Open the Databricks App UI and walk through risk dashboard + chat
5. Highlight governance, reproducibility, and extensibility

## Why This Matters

RiskBricks demonstrates how to operationalize agentic analytics in a regulated financial setting using native Databricks capabilities: data engineering, governance, model lifecycle, agent orchestration, and application delivery in one platform.

---

Built for Data + AI Summit style technical storytelling with a direct path to production-grade portfolio risk intelligence.# riskbricks
