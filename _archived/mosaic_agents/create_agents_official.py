# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Create Mosaic AI Agents - Official Databricks Approach
# MAGIC
# MAGIC **Based on:** Official Databricks Training Materials
# MAGIC
# MAGIC This notebook uses the **exact approach** from Databricks GenAI training courses.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Required Packages
# MAGIC
# MAGIC Using the exact versions from Databricks official training.

# COMMAND ----------

# DBTITLE 1,Install Databricks Agent Framework
# MAGIC %pip install -U -qqq mlflow-skinny[databricks]==3.4.0 databricks-langchain==0.8.0 langchain==0.3.7 langchain-community==0.3.7 databricks-agents mlflow[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

import mlflow
from databricks_langchain import ChatDatabricks
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain import hub

# Enable MLflow Auto-Log for tracing
mlflow.langchain.autolog()

print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Define Unity Catalog Tools
# MAGIC
# MAGIC Load UC SQL functions as tools for the agents.

# COMMAND ----------

from langchain_community.tools.databricks import UCFunctionToolkit

# IMPORTANT: Replace with your SQL Warehouse ID
# Find it at: SQL Warehouses → Click your warehouse → Copy ID from URL
WAREHOUSE_ID = "YOUR_WAREHOUSE_ID"  # ← CHANGE THIS!

# Create toolkit from Unity Catalog functions
toolkit = UCFunctionToolkit(warehouse_id=WAREHOUSE_ID)

# Load ALL your custom UC functions
all_tools = toolkit.include(
    "riskbricks.tools.get_company_info",
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_forecast_consensus",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_decision_signal",
    "riskbricks.tools.get_earnings_surprise",
    "riskbricks.tools.get_analyst_ratings",
    "riskbricks.tools.get_top_opportunities",
    "riskbricks.tools.get_portfolio_summary",
    "riskbricks.tools.get_sector_risk_summary"
)

print(f"✅ Loaded {len(all_tools.tools)} UC functions as tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Create Forecast Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Brain (LLM)

# COMMAND ----------

# Use Llama 3.3 70B as the brain - official Databricks approach
llm_forecast = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct", 
    max_tokens=2500,
    temperature=0.3
)

print("✅ Forecast Agent brain defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Tools

# COMMAND ----------

# Create forecast-specific toolkit
forecast_toolkit = UCFunctionToolkit(warehouse_id=WAREHOUSE_ID)
forecast_tools = forecast_toolkit.include(
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_forecast_consensus",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_company_info"
).tools

print(f"✅ Forecast Agent has {len(forecast_tools)} tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Planning Logic (Prompt)

# COMMAND ----------

# Pull ReAct-style prompt from LangChain Hub (official approach)
forecast_prompt = hub.pull("hwchase17/openai-functions-agent")

# Customize the system message
forecast_prompt.messages[0].prompt.template = """You are a quantitative finance expert specializing in stock price forecasting.
You have access to multiple forecasting models (GBM, Ridge, Mean, News Event).

When asked about forecasts:
1. Query forecast data using tools
2. Analyze consensus across models
3. Consider risk metrics and volatility
4. Provide confidence intervals
5. Explain methodology and discrepancies
6. Give actionable insights

Always be transparent about model limitations."""

print("✅ Forecast Agent prompt configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Agent

# COMMAND ----------

# Create agent using official Databricks pattern
forecast_agent = create_tool_calling_agent(llm_forecast, forecast_tools, forecast_prompt)
forecast_executor = AgentExecutor(
    agent=forecast_agent, 
    tools=forecast_tools, 
    verbose=True, 
    handle_parsing_errors=True,
    max_iterations=5
)

print("✅ Forecast Agent created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Locally

# COMMAND ----------

# Test the agent before deploying
test_response = forecast_executor.invoke({"input": "What is the forecast for AAPL?"})
print(test_response["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register and Deploy

# COMMAND ----------

# Set MLflow registry to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Define model name in UC
forecast_model_name = "riskbricks.agents.forecast_agent"

# Log the agent to MLflow
with mlflow.start_run(run_name="forecast_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=forecast_executor,
        artifact_path="forecast_agent",
        input_example={"input": "What is the forecast for NVDA?"},
    )

# Register in Unity Catalog
forecast_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=forecast_model_name
)

print(f"✅ Forecast Agent registered: {forecast_model_name} v{forecast_model_version.version}")

# COMMAND ----------

# Deploy to Model Serving using databricks-agents
from databricks import agents

deployment_info = agents.deploy(
    model_name=forecast_model_name,
    model_version=forecast_model_version.version,
    endpoint_name="riskbricks_forecast_agent"
)

print(f"✅ Forecast Agent deployed to endpoint: riskbricks_forecast_agent")
print(f"   Endpoint URL: {deployment_info.endpoint_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Create Risk Agent

# COMMAND ----------

# Define Brain
llm_risk = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct", 
    max_tokens=2500,
    temperature=0.2  # Lower temperature for risk analysis
)

# Define Tools
risk_toolkit = UCFunctionToolkit(warehouse_id=WAREHOUSE_ID)
risk_tools = risk_toolkit.include(
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_sector_risk_summary",
    "riskbricks.tools.get_company_info"
).tools

# Define Prompt
risk_prompt = hub.pull("hwchase17/openai-functions-agent")
risk_prompt.messages[0].prompt.template = """You are a risk management expert specializing in portfolio risk analytics.

You calculate and interpret:
- Volatility (Historical, EWMA, forward-looking)
- Value at Risk (VaR) at 95% and 99%
- Expected Shortfall (ES)
- Beta and market sensitivity
- Maximum Drawdown
- Market Impact and liquidity

Always provide context and actionable recommendations."""

# Create Agent
risk_agent = create_tool_calling_agent(llm_risk, risk_tools, risk_prompt)
risk_executor = AgentExecutor(
    agent=risk_agent, 
    tools=risk_tools, 
    verbose=True, 
    handle_parsing_errors=True,
    max_iterations=5
)

print("✅ Risk Agent created")

# COMMAND ----------

# Test Risk Agent
test_response = risk_executor.invoke({"input": "What is the risk profile of NVDA?"})
print(test_response["output"])

# COMMAND ----------

# Log and Deploy Risk Agent
mlflow.set_registry_uri("databricks-uc")
risk_model_name = "riskbricks.agents.risk_agent"

with mlflow.start_run(run_name="risk_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=risk_executor,
        artifact_path="risk_agent",
        input_example={"input": "What is the risk profile of TSLA?"},
    )

risk_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=risk_model_name
)

deployment_info = agents.deploy(
    model_name=risk_model_name,
    model_version=risk_model_version.version,
    endpoint_name="riskbricks_risk_agent"
)

print(f"✅ Risk Agent deployed to endpoint: riskbricks_risk_agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Create Decision Agent

# COMMAND ----------

# Define Brain
llm_decision = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct", 
    max_tokens=2500,
    temperature=0.3
)

# Define Tools
decision_toolkit = UCFunctionToolkit(warehouse_id=WAREHOUSE_ID)
decision_tools = decision_toolkit.include(
    "riskbricks.tools.get_decision_signal",
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_earnings_surprise",
    "riskbricks.tools.get_analyst_ratings",
    "riskbricks.tools.get_company_info"
).tools

# Define Prompt
decision_prompt = hub.pull("hwchase17/openai-functions-agent")
decision_prompt.messages[0].prompt.template = """You are an investment decision expert generating BUY/SELL/HOLD signals.

Decision framework:
- STRONG BUY: >3% return, low risk, positive catalysts
- BUY: >1% return, acceptable risk
- HOLD: -1% to +1% or mixed signals
- SELL: <-1% return
- STRONG SELL: <-3% return, high risk

Always explain reasoning, quantify confidence, and suggest position sizing."""

# Create Agent
decision_agent = create_tool_calling_agent(llm_decision, decision_tools, decision_prompt)
decision_executor = AgentExecutor(
    agent=decision_agent, 
    tools=decision_tools, 
    verbose=True, 
    handle_parsing_errors=True,
    max_iterations=5
)

print("✅ Decision Agent created")

# COMMAND ----------

# Test Decision Agent
test_response = decision_executor.invoke({"input": "Should I buy MSFT?"})
print(test_response["output"])

# COMMAND ----------

# Log and Deploy Decision Agent
mlflow.set_registry_uri("databricks-uc")
decision_model_name = "riskbricks.agents.decision_agent"

with mlflow.start_run(run_name="decision_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=decision_executor,
        artifact_path="decision_agent",
        input_example={"input": "Should I buy GOOGL?"},
    )

decision_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=decision_model_name
)

deployment_info = agents.deploy(
    model_name=decision_model_name,
    model_version=decision_model_version.version,
    endpoint_name="riskbricks_decision_agent"
)

print(f"✅ Decision Agent deployed to endpoint: riskbricks_decision_agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Create Supervisor Agent

# COMMAND ----------

# Define Brain
llm_supervisor = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct", 
    max_tokens=3000,
    temperature=0.4  # Higher creativity for synthesis
)

# Define Tools (all portfolio-level tools)
supervisor_toolkit = UCFunctionToolkit(warehouse_id=WAREHOUSE_ID)
supervisor_tools = supervisor_toolkit.include(
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_decision_signal",
    "riskbricks.tools.get_top_opportunities",
    "riskbricks.tools.get_portfolio_summary",
    "riskbricks.tools.get_sector_risk_summary"
).tools

# Define Prompt
supervisor_prompt = hub.pull("hwchase17/openai-functions-agent")
supervisor_prompt.messages[0].prompt.template = """You are the Chief Investment Officer coordinating specialist AI agents.

Your team:
- Forecast Agent: Price predictions
- Risk Agent: Risk analytics
- Decision Agent: BUY/SELL/HOLD signals

Your role:
- Coordinate agents for complex questions
- Synthesize multi-agent insights
- Provide portfolio-level analysis
- Generate executive summaries

Always delegate to specialists and add strategic overlay."""

# Create Agent
supervisor_agent = create_tool_calling_agent(llm_supervisor, supervisor_tools, supervisor_prompt)
supervisor_executor = AgentExecutor(
    agent=supervisor_agent, 
    tools=supervisor_tools, 
    verbose=True, 
    handle_parsing_errors=True,
    max_iterations=8  # More iterations for complex queries
)

print("✅ Supervisor Agent created")

# COMMAND ----------

# Test Supervisor Agent
test_response = supervisor_executor.invoke({"input": "What are the top 3 investment opportunities?"})
print(test_response["output"])

# COMMAND ----------

# Log and Deploy Supervisor Agent
mlflow.set_registry_uri("databricks-uc")
supervisor_model_name = "riskbricks.agents.supervisor"

with mlflow.start_run(run_name="supervisor_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=supervisor_executor,
        artifact_path="supervisor_agent",
        input_example={"input": "Give me a portfolio analysis"},
    )

supervisor_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=supervisor_model_name
)

deployment_info = agents.deploy(
    model_name=supervisor_model_name,
    model_version=supervisor_model_version.version,
    endpoint_name="riskbricks_supervisor"
)

print(f"✅ Supervisor Agent deployed to endpoint: riskbricks_supervisor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Deployment Complete!

# COMMAND ----------

print("""
🎉 All 4 Agents Deployed Successfully!

Endpoints created:
1. riskbricks_forecast_agent
2. riskbricks_risk_agent  
3. riskbricks_decision_agent
4. riskbricks_supervisor

Models registered in Unity Catalog:
1. riskbricks.agents.forecast_agent
2. riskbricks.agents.risk_agent
3. riskbricks.agents.decision_agent
4. riskbricks.agents.supervisor

Next steps:
1. Go to Machine Learning → Serving to view endpoints
2. Wait for all endpoints to become "Ready" (~5-10 min)
3. Test queries on the endpoints
4. Review MLflow traces for debugging
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test All Deployed Endpoints

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Test Forecast Agent
print("1️⃣ Testing Forecast Agent...")
response = w.serving_endpoints.query(
    name="riskbricks_forecast_agent",
    inputs=[{"input": "What is the forecast for AAPL?"}]
)
print(response)
print("\n" + "="*80 + "\n")

# Test Risk Agent
print("2️⃣ Testing Risk Agent...")
response = w.serving_endpoints.query(
    name="riskbricks_risk_agent",
    inputs=[{"input": "What is the risk profile of NVDA?"}]
)
print(response)
print("\n" + "="*80 + "\n")

# Test Decision Agent
print("3️⃣ Testing Decision Agent...")
response = w.serving_endpoints.query(
    name="riskbricks_decision_agent",
    inputs=[{"input": "Should I buy MSFT?"}]
)
print(response)
print("\n" + "="*80 + "\n")

# Test Supervisor Agent
print("4️⃣ Testing Supervisor Agent...")
response = w.serving_endpoints.query(
    name="riskbricks_supervisor",
    inputs=[{"input": "What are the top 3 investment opportunities?"}]
)
print(response)

# COMMAND ----------

dbutils.notebook.exit("success")
