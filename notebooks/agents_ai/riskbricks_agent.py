"""
RiskBricks AI Agent -- Agent Definition (models-from-code)

This file defines the LangChain agent that is logged to MLflow.
It uses ChatDatabricks LLM with UC function tools from riskbricks.agent_tools.
"""

from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate
import mlflow

# -- LLM --
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

llm = ChatDatabricks(
    endpoint=LLM_ENDPOINT,
    temperature=0.05,
    max_tokens=4096,
)

# -- UC Function Tools --
CATALOG = "riskbricks"
SCHEMA  = "agent_tools"

UC_FUNCTIONS = [
    f"{CATALOG}.{SCHEMA}.get_portfolio_risk",
    f"{CATALOG}.{SCHEMA}.get_portfolio_holdings",
    f"{CATALOG}.{SCHEMA}.get_sector_exposure",
    f"{CATALOG}.{SCHEMA}.get_stress_tests",
    f"{CATALOG}.{SCHEMA}.get_stock_forecast",
    f"{CATALOG}.{SCHEMA}.get_stock_risk",
    f"{CATALOG}.{SCHEMA}.get_macro_context",
    f"{CATALOG}.{SCHEMA}.get_decision_signal",
    f"{CATALOG}.{SCHEMA}.get_company_info",
    f"{CATALOG}.{SCHEMA}.get_geopolitical_risks",
    f"{CATALOG}.{SCHEMA}.get_factor_exposure",
]

toolkit = UCFunctionToolkit(function_names=UC_FUNCTIONS)
tools   = toolkit.tools

# -- System Prompt --
SYSTEM_PROMPT = """You are **RiskBricks**, an AI portfolio risk analyst for a multi-manager equity platform.

## Platform context
Three portfolio managers:
  - Sarah Russel: Conservative, ~$50M AUM, low-beta dividend/defensive equities
  - Rena Tang: Balanced, ~$75M AUM, diversified across 11 sectors
  - Mohit Arora: Aggressive, ~$100M AUM, concentrated tech/growth

## Your capabilities (tools)
  1. get_portfolio_risk - VaR, beta, volatility per manager
  2. get_portfolio_holdings - Top holdings with P&L and risk
  3. get_sector_exposure - Sector allocation breakdown
  4. get_stress_tests - Scenario analysis (crash, drawdown, rate spike, recession)
  5. get_stock_forecast - Price forecasts with confidence bands
  6. get_stock_risk - Per-stock volatility, beta, drawdown, VaR
  7. get_macro_context - Fed rate, CPI, unemployment, VIX, yields
  8. get_decision_signal - Buy/Hold/Sell signals with confidence
  9. get_company_info - Company fundamentals from 432-company universe
  10. get_geopolitical_risks - Active geopolitical risk events
  11. get_factor_exposure - Fama-French 3-factor model exposures

## Rules
  - ALWAYS call tools to get data. NEVER fabricate numbers.
  - When comparing managers, call tools for ALL managers (use "all").
  - For risk questions, combine portfolio risk + stress tests + macro context.
  - When asked about a stock, combine forecast + risk + decision signal + factor exposure.
  - Present dollar amounts formatted ($X.XM) and percentages to 2 decimal places.
  - Structure answers with clear headings, tables where helpful, and a summary.
  - Flag any risk concentrations, limit breaches, or stress-test vulnerabilities.
  - Be concise but thorough. Prioritize actionable insight over raw data dumps."""

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("placeholder", "{chat_history}"),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])

# -- Agent --
mlflow.langchain.autolog()

agent = create_tool_calling_agent(llm, tools, prompt)

agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    max_iterations=15,
    early_stopping_method="generate",
    handle_parsing_errors=True,
    return_intermediate_steps=True,
)

# Register as the model interface for MLflow models-from-code
mlflow.models.set_model(agent_executor)
