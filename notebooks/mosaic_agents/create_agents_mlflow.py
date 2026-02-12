# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Create Mosaic AI Agents with MLflow
# MAGIC 
# MAGIC **Official Databricks Approach:** Use MLflow + databricks-agents
# MAGIC 
# MAGIC Based on: https://docs.databricks.com/en/generative-ai/agent-framework/create-agent.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Required Packages

# COMMAND ----------

# DBTITLE 1,Install Databricks Agent Framework
%pip install -U -q databricks-agents mlflow mlflow[databricks] langchain==0.1.20 langchain-community==0.0.38 langchain-core==0.1.52
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

import mlflow
import pandas as pd
from databricks import agents
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# Enable MLflow tracing for debugging
mlflow.langchain.autolog()

# Initialize Databricks client
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Define Tools from Unity Catalog

# COMMAND ----------

from langchain_community.tools.databricks import UCFunctionToolkit

# Create toolkit from Unity Catalog functions
toolkit = UCFunctionToolkit(warehouse_id="YOUR_WAREHOUSE_ID")

# Load your custom UC functions
tools = toolkit.include(
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

print(f"✅ Loaded {len(tools.tools)} UC functions as tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Create Forecast Agent

# COMMAND ----------

from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.prompts import PromptTemplate

# Define system prompt for Forecast Agent (ReAct format)
forecast_prompt = PromptTemplate.from_template("""You are a quantitative finance expert specializing in stock price forecasting.
You have access to multiple forecasting models (GBM, Ridge, Mean, News Event).

When asked about forecasts:
1. Query forecast data using tools
2. Analyze consensus across models
3. Consider risk metrics and volatility
4. Provide confidence intervals
5. Explain methodology and discrepancies
6. Give actionable insights

Always be transparent about model limitations.

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Question: {input}
Thought: {agent_scratchpad}""")

# Connect to Databricks Foundation Model
llm = ChatOpenAI(
    base_url=f"{mlflow.utils.databricks_utils.get_databricks_host_creds().host}/serving-endpoints",
    api_key=mlflow.utils.databricks_utils.get_databricks_host_creds().token,
    model="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.3
)

# Create agent with Forecast-specific tools
forecast_toolkit = UCFunctionToolkit(warehouse_id="YOUR_WAREHOUSE_ID")
forecast_tools = forecast_toolkit.include(
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_forecast_consensus",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_company_info"
).tools

forecast_agent = create_react_agent(llm, forecast_tools, forecast_prompt)
forecast_executor = AgentExecutor(agent=forecast_agent, tools=forecast_tools, verbose=True, handle_parsing_errors=True)

print("✅ Forecast Agent created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Forecast Agent Locally

# COMMAND ----------

# Test locally before deploying
test_result = forecast_executor.invoke({"input": "What is the forecast for AAPL?"})
print(test_result["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy Forecast Agent to Model Serving

# COMMAND ----------

# Log the agent as an MLflow model
with mlflow.start_run(run_name="forecast_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=forecast_executor,
        artifact_path="forecast_agent",
        input_example="What is the forecast for NVDA?",
    )

# Register the model
forecast_model_name = "riskbricks.agents.forecast_agent"
forecast_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=forecast_model_name
)

print(f"✅ Forecast Agent registered: {forecast_model_name} v{forecast_model_version.version}")

# COMMAND ----------

# Deploy to serving endpoint using databricks-agents
agents.deploy(
    model_name=forecast_model_name,
    model_version=forecast_model_version.version,
    endpoint_name="riskbricks_forecast_agent"
)

print("✅ Forecast Agent deployed to serving endpoint: riskbricks_forecast_agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Create Risk Agent

# COMMAND ----------

# Define system prompt for Risk Agent (ReAct format)
risk_prompt = PromptTemplate.from_template("""You are a risk management expert specializing in portfolio risk analytics.

You calculate and interpret:
- Volatility (Historical, EWMA, forward-looking)
- Value at Risk (VaR) at 95% and 99%
- Expected Shortfall (ES)
- Beta and market sensitivity
- Maximum Drawdown
- Market Impact and liquidity

Always provide context and actionable recommendations.

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Question: {input}
Thought: {agent_scratchpad}""")

# LLM with different temperature for Risk Agent
risk_llm = ChatOpenAI(
    base_url=f"{mlflow.utils.databricks_utils.get_databricks_host_creds().host}/serving-endpoints",
    api_key=mlflow.utils.databricks_utils.get_databricks_host_creds().token,
    model="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.2
)

# Create agent with Risk-specific tools
risk_toolkit = UCFunctionToolkit(warehouse_id="YOUR_WAREHOUSE_ID")
risk_tools = risk_toolkit.include(
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_sector_risk_summary",
    "riskbricks.tools.get_company_info"
).tools

risk_agent = create_react_agent(risk_llm, risk_tools, risk_prompt)
risk_executor = AgentExecutor(agent=risk_agent, tools=risk_tools, verbose=True, handle_parsing_errors=True)

print("✅ Risk Agent created")

# COMMAND ----------

# Test Risk Agent
test_result = risk_executor.invoke({"input": "What is the risk profile of NVDA?"})
print(test_result["output"])

# COMMAND ----------

# Log and deploy Risk Agent
with mlflow.start_run(run_name="risk_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=risk_executor,
        artifact_path="risk_agent",
        input_example="What is the risk profile of TSLA?",
    )

risk_model_name = "riskbricks.agents.risk_agent"
risk_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=risk_model_name
)

agents.deploy(
    model_name=risk_model_name,
    model_version=risk_model_version.version,
    endpoint_name="riskbricks_risk_agent"
)

print("✅ Risk Agent deployed to serving endpoint: riskbricks_risk_agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Create Decision Agent

# COMMAND ----------

# Define system prompt for Decision Agent (ReAct format)
decision_prompt = PromptTemplate.from_template("""You are an investment decision expert generating BUY/SELL/HOLD signals.

Decision framework:
- STRONG BUY: >3% return, low risk, positive catalysts
- BUY: >1% return, acceptable risk
- HOLD: -1% to +1% or mixed signals
- SELL: <-1% return
- STRONG SELL: <-3% return, high risk

Always explain reasoning, quantify confidence, and suggest position sizing.

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Question: {input}
Thought: {agent_scratchpad}""")

decision_llm = ChatOpenAI(
    base_url=f"{mlflow.utils.databricks_utils.get_databricks_host_creds().host}/serving-endpoints",
    api_key=mlflow.utils.databricks_utils.get_databricks_host_creds().token,
    model="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.3
)

# Create agent with Decision-specific tools
decision_toolkit = UCFunctionToolkit(warehouse_id="YOUR_WAREHOUSE_ID")
decision_tools = decision_toolkit.include(
    "riskbricks.tools.get_decision_signal",
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_earnings_surprise",
    "riskbricks.tools.get_analyst_ratings",
    "riskbricks.tools.get_company_info"
).tools

decision_agent = create_react_agent(decision_llm, decision_tools, decision_prompt)
decision_executor = AgentExecutor(agent=decision_agent, tools=decision_tools, verbose=True, handle_parsing_errors=True)

print("✅ Decision Agent created")

# COMMAND ----------

# Test Decision Agent
test_result = decision_executor.invoke({"input": "Should I buy MSFT?"})
print(test_result["output"])

# COMMAND ----------

# Log and deploy Decision Agent
with mlflow.start_run(run_name="decision_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=decision_executor,
        artifact_path="decision_agent",
        input_example="Should I buy GOOGL?",
    )

decision_model_name = "riskbricks.agents.decision_agent"
decision_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=decision_model_name
)

agents.deploy(
    model_name=decision_model_name,
    model_version=decision_model_version.version,
    endpoint_name="riskbricks_decision_agent"
)

print("✅ Decision Agent deployed to serving endpoint: riskbricks_decision_agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Create Supervisor Agent

# COMMAND ----------

# Define system prompt for Supervisor Agent (ReAct format)
supervisor_prompt = PromptTemplate.from_template("""You are the Chief Investment Officer coordinating specialist AI agents.

Your team:
- Forecast Agent: Price predictions
- Risk Agent: Risk analytics
- Decision Agent: BUY/SELL/HOLD signals

Your role:
- Coordinate agents for complex questions
- Synthesize multi-agent insights
- Provide portfolio-level analysis
- Generate executive summaries

Always delegate to specialists and add strategic overlay.

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Question: {input}
Thought: {agent_scratchpad}""")

supervisor_llm = ChatOpenAI(
    base_url=f"{mlflow.utils.databricks_utils.get_databricks_host_creds().host}/serving-endpoints",
    api_key=mlflow.utils.databricks_utils.get_databricks_host_creds().token,
    model="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.4
)

# Create agent with all portfolio-level tools
supervisor_toolkit = UCFunctionToolkit(warehouse_id="YOUR_WAREHOUSE_ID")
supervisor_tools = supervisor_toolkit.include(
    "riskbricks.tools.get_latest_forecast",
    "riskbricks.tools.get_risk_metrics",
    "riskbricks.tools.get_decision_signal",
    "riskbricks.tools.get_top_opportunities",
    "riskbricks.tools.get_portfolio_summary",
    "riskbricks.tools.get_sector_risk_summary"
).tools

supervisor_agent = create_react_agent(supervisor_llm, supervisor_tools, supervisor_prompt)
supervisor_executor = AgentExecutor(agent=supervisor_agent, tools=supervisor_tools, verbose=True, handle_parsing_errors=True)

print("✅ Supervisor Agent created")

# COMMAND ----------

# Test Supervisor Agent
test_result = supervisor_executor.invoke({"input": "What are the top 3 investment opportunities?"})
print(test_result["output"])

# COMMAND ----------

# Log and deploy Supervisor Agent
with mlflow.start_run(run_name="supervisor_agent"):
    logged_agent_info = mlflow.langchain.log_model(
        lc_model=supervisor_executor,
        artifact_path="supervisor_agent",
        input_example="Give me a portfolio analysis",
    )

supervisor_model_name = "riskbricks.agents.supervisor"
supervisor_model_version = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=supervisor_model_name
)

agents.deploy(
    model_name=supervisor_model_name,
    model_version=supervisor_model_version.version,
    endpoint_name="riskbricks_supervisor"
)

print("✅ Supervisor Agent deployed to serving endpoint: riskbricks_supervisor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verification

# COMMAND ----------

print("""
🎉 All 4 Agents Deployed!

Endpoints:
1. riskbricks_forecast_agent
2. riskbricks_risk_agent
3. riskbricks_decision_agent
4. riskbricks_supervisor

To query an agent:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()

response = client.responses.create(
    model="riskbricks_supervisor",
    input=[{"role": "user", "content": "What are the top opportunities?"}]
)
print(response.choices[0].message.content)
```
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test All Endpoints

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()

# Test Forecast Agent
print("1️⃣ Testing Forecast Agent...")
response = client.responses.create(
    model="riskbricks_forecast_agent",
    input=[{"role": "user", "content": "What is the forecast for AAPL?"}]
)
print(response.choices[0].message.content)
print("\n" + "="*80 + "\n")

# Test Risk Agent
print("2️⃣ Testing Risk Agent...")
response = client.responses.create(
    model="riskbricks_risk_agent",
    input=[{"role": "user", "content": "What is the risk profile of NVDA?"}]
)
print(response.choices[0].message.content)
print("\n" + "="*80 + "\n")

# Test Decision Agent
print("3️⃣ Testing Decision Agent...")
response = client.responses.create(
    model="riskbricks_decision_agent",
    input=[{"role": "user", "content": "Should I buy MSFT?"}]
)
print(response.choices[0].message.content)
print("\n" + "="*80 + "\n")

# Test Supervisor Agent
print("4️⃣ Testing Supervisor Agent...")
response = client.responses.create(
    model="riskbricks_supervisor",
    input=[{"role": "user", "content": "What are the top 3 investment opportunities?"}]
)
print(response.choices[0].message.content)

# COMMAND ----------

dbutils.notebook.exit("success")
