# RiskBricks AI Agents

## Architecture

**Supervisor → 6 Sub-Agents** using LangGraph + Mosaic AI Agent Framework.

```
User Question
    │
    ▼
Supervisor Agent (Llama 3.3 70B)
    │
    ├──▶ Risk Agent           → get_portfolio_risk_metrics, get_stress_test_results
    ├──▶ Price Target Agent   → get_stock_forecast
    ├──▶ ML Direction Agent   → get_ml_stock_forecast, get_ml_market_overview
    ├──▶ Factor Agent         → get_factor_exposures, get_sector_exposures
    ├──▶ Decision Agent       → get_decision_signal, get_macro_context
    └──▶ News Agent           → get_news_context, get_portfolio_holdings
```

## Deployment

**Endpoint**: `riskbricks-supervisor-agent` (always-on)
**Model**: `riskbricks.agents.riskbricks_agent`
**LLM**: Llama 3.3 70B (Databricks Foundation Model API)

### Steps

1. Register UC functions: `notebooks/agents/01_register_uc_tools.py`
2. Create agent model: `notebooks/agents/02_create_agent.py`
3. Deploy to endpoint: `notebooks/agents/03_deploy_agent.py`

## Agent Files

| File | Purpose |
|------|---------|
| `notebooks/agents/riskbricks_agent.py` | Agent logic: 6 sub-agents, supervisor routing, prompts |
| `notebooks/agents/01_register_uc_tools.py` | Registers 11 UC functions in `{catalog}.agent_tools` |
| `notebooks/agents/02_create_agent.py` | Creates agent model + logs to Unity Catalog |
| `notebooks/agents/03_deploy_agent.py` | Deploys agent to serving endpoint |

## 11 UC Functions

| Function | Gold Table Queried | Agent |
|----------|--------------------|-------|
| `get_portfolio_risk_metrics` | portfolio_risk_metrics | Risk |
| `get_stress_test_results` | stress_test_results | Risk |
| `get_stock_forecast` | stock_forecasts | Price Target |
| `get_ml_stock_forecast` | ml_stock_predictions | ML Direction |
| `get_ml_market_overview` | ml_stock_predictions | ML Direction |
| `get_factor_exposures` | risk_factor_exposures | Factor |
| `get_sector_exposures` | sector_exposures | Factor |
| `get_decision_signal` | decision_signals | Decision |
| `get_macro_context` | macro_indicators_daily | Decision |
| `get_news_context` | bronze.news_rss_all | News |
| `get_portfolio_holdings` | portfolio_holdings, portfolio_managers | News |

## Example Interaction

```
User: "Should I sell NVDA?"

1. Supervisor routes to Decision Agent → calls get_decision_signal('NVDA')
2. Supervisor routes to ML Direction Agent → calls get_ml_stock_forecast('NVDA')
3. Supervisor synthesizes: "HOLD NVDA — composite score 72/100, ML predicts UP with 68% confidence"
```
