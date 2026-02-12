# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Deploy RiskBricks AI Agents - Official Databricks Pattern
# MAGIC 
# MAGIC **This notebook uses the OFFICIAL Databricks pattern for production AI agents:**
# MAGIC - MLflow's `ResponsesAgent` (not LangChain)
# MAGIC - `databricks-openai.UCFunctionToolkit` (not langchain_community)
# MAGIC - `databricks.agents.deploy()` (official deployment SDK)
# MAGIC 
# MAGIC **Based on:** Databricks "Get Started with AI Agents" training course
# MAGIC 
# MAGIC This is the exact pattern that AI Playground exports for production use.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Required Packages

# COMMAND ----------

# DBTITLE 1,Install Official Databricks Agent Packages
%pip install -U -qqq databricks-agents databricks-sdk databricks-openai backoff mlflow
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

# Unity Catalog configuration
CATALOG = "riskbricks"
SCHEMA = "agents"
TOOLS_SCHEMA = "tools"

# Foundation Model endpoint
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# UC Tools for each agent
FORECAST_TOOLS = [
    f"{CATALOG}.{TOOLS_SCHEMA}.get_latest_forecast",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_forecast_consensus",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_risk_metrics",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_company_info"
]

RISK_TOOLS = [
    f"{CATALOG}.{TOOLS_SCHEMA}.get_risk_metrics",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_sector_risk_summary",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_company_info"
]

DECISION_TOOLS = [
    f"{CATALOG}.{TOOLS_SCHEMA}.get_decision_signal",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_latest_forecast",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_risk_metrics",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_earnings_surprise",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_analyst_ratings",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_company_info"
]

SUPERVISOR_TOOLS = [
    f"{CATALOG}.{TOOLS_SCHEMA}.get_latest_forecast",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_risk_metrics",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_decision_signal",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_top_opportunities",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_portfolio_summary",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_sector_risk_summary"
]

print("✅ Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Define System Prompts

# COMMAND ----------

FORECAST_PROMPT = """You are a quantitative finance expert specializing in stock price forecasting.
You have access to multiple forecasting models (GBM, Ridge, Mean, News Event).

When asked about forecasts:
1. Query forecast data using tools
2. Analyze consensus across models
3. Consider risk metrics and volatility
4. Provide confidence intervals
5. Explain methodology and discrepancies
6. Give actionable insights

Always be transparent about model limitations."""

RISK_PROMPT = """You are a risk management expert specializing in portfolio risk analytics.

You calculate and interpret:
- Volatility (Historical, EWMA, forward-looking)
- Value at Risk (VaR) at 95% and 99%
- Expected Shortfall (ES)
- Beta and market sensitivity
- Maximum Drawdown
- Market Impact and liquidity

Always provide context and actionable recommendations."""

DECISION_PROMPT = """You are an investment decision expert generating BUY/SELL/HOLD signals.

Decision framework:
- STRONG BUY: >3% return, low risk, positive catalysts
- BUY: >1% return, acceptable risk
- HOLD: -1% to +1% or mixed signals
- SELL: <-1% return
- STRONG SELL: <-3% return, high risk

Always explain reasoning, quantify confidence, and suggest position sizing."""

SUPERVISOR_PROMPT = """You are the Chief Investment Officer coordinating specialist AI agents.

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

print("✅ System prompts defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔨 Write Agent Base Class

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write agent_base.py
# MAGIC 
# MAGIC This is the core ResponsesAgent implementation that all agents will use.

# COMMAND ----------

# MAGIC %%writefile agent_base.py
# MAGIC """
# MAGIC RiskBricks Agent Base - Official Databricks ResponsesAgent Pattern
# MAGIC 
# MAGIC This module implements the base agent class using MLflow's ResponsesAgent,
# MAGIC following the exact pattern from Databricks AI Playground export.
# MAGIC 
# MAGIC Based on: Databricks "Get Started with AI Agents" training course
# MAGIC """
# MAGIC 
# MAGIC import json
# MAGIC from typing import Any, Callable, Generator, Optional
# MAGIC from uuid import uuid4
# MAGIC import warnings
# MAGIC 
# MAGIC import mlflow
# MAGIC import openai
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks_openai import UCFunctionToolkit
# MAGIC from mlflow.entities import SpanType
# MAGIC from mlflow.pyfunc import ResponsesAgent
# MAGIC from mlflow.types.responses import (
# MAGIC     ResponsesAgentRequest,
# MAGIC     ResponsesAgentResponse,
# MAGIC     ResponsesAgentStreamEvent,
# MAGIC     output_to_responses_items_stream,
# MAGIC     to_chat_completions_input,
# MAGIC )
# MAGIC from openai import OpenAI
# MAGIC from pydantic import BaseModel
# MAGIC from unitycatalog.ai.core.base import get_uc_function_client
# MAGIC 
# MAGIC 
# MAGIC class ToolInfo(BaseModel):
# MAGIC     """
# MAGIC     Class representing a tool for the agent.
# MAGIC     - "name" (str): The name of the tool.
# MAGIC     - "spec" (dict): JSON description of the tool (matches OpenAI Responses format)
# MAGIC     - "exec_fn" (Callable): Function that implements the tool logic
# MAGIC     """
# MAGIC     name: str
# MAGIC     spec: dict
# MAGIC     exec_fn: Callable
# MAGIC 
# MAGIC 
# MAGIC def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None, uc_function_client=None):
# MAGIC     """
# MAGIC     Create a ToolInfo object from a tool specification.
# MAGIC     
# MAGIC     Args:
# MAGIC         tool_spec: Tool specification from UCFunctionToolkit
# MAGIC         exec_fn_param: Optional custom execution function
# MAGIC         uc_function_client: UC function execution client
# MAGIC     
# MAGIC     Returns:
# MAGIC         ToolInfo object
# MAGIC     """
# MAGIC     # Remove 'strict' if present
# MAGIC     tool_spec["function"].pop("strict", None)
# MAGIC     tool_name = tool_spec["function"]["name"]
# MAGIC     udf_name = tool_name.replace("__", ".")
# MAGIC 
# MAGIC     # Define a wrapper that accepts kwargs for the UC tool call,
# MAGIC     # then passes them to the UC tool execution client
# MAGIC     def exec_fn(**kwargs):
# MAGIC         function_result = uc_function_client.execute_function(udf_name, kwargs)
# MAGIC         if function_result.error is not None:
# MAGIC             return function_result.error
# MAGIC         else:
# MAGIC             return function_result.value
# MAGIC     
# MAGIC     return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)
# MAGIC 
# MAGIC 
# MAGIC class RiskBricksAgent(ResponsesAgent):
# MAGIC     """
# MAGIC     RiskBricks Agent - Tool-calling agent using Databricks Foundation Models
# MAGIC     
# MAGIC     This agent uses the official Databricks pattern:
# MAGIC     - MLflow ResponsesAgent base class
# MAGIC     - databricks-openai UCFunctionToolkit for UC tools
# MAGIC     - OpenAI SDK with Databricks endpoints
# MAGIC     - Proper tracing and error handling
# MAGIC     """
# MAGIC 
# MAGIC     def __init__(self, llm_endpoint: str, tools: list[ToolInfo], system_prompt: str = ""):
# MAGIC         """
# MAGIC         Initialize the RiskBricks Agent.
# MAGIC         
# MAGIC         Args:
# MAGIC             llm_endpoint: Name of the Databricks Foundation Model endpoint
# MAGIC             tools: List of ToolInfo objects
# MAGIC             system_prompt: System prompt for the agent
# MAGIC         """
# MAGIC         self.llm_endpoint = llm_endpoint
# MAGIC         self.system_prompt = system_prompt
# MAGIC         self.workspace_client = WorkspaceClient()
# MAGIC         self.model_serving_client: OpenAI = (
# MAGIC             self.workspace_client.serving_endpoints.get_open_ai_client()
# MAGIC         )
# MAGIC         self._tools_dict = {tool.name: tool for tool in tools}
# MAGIC 
# MAGIC     def get_tool_specs(self) -> list[dict]:
# MAGIC         """Returns tool specifications in the format OpenAI expects."""
# MAGIC         return [tool_info.spec for tool_info in self._tools_dict.values()]
# MAGIC 
# MAGIC     @mlflow.trace(span_type=SpanType.TOOL)
# MAGIC     def execute_tool(self, tool_name: str, args: dict) -> Any:
# MAGIC         """Executes the specified tool with the given arguments."""
# MAGIC         return self._tools_dict[tool_name].exec_fn(**args)
# MAGIC 
# MAGIC     def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
# MAGIC         """
# MAGIC         Call the LLM with messages and tools.
# MAGIC         
# MAGIC         Args:
# MAGIC             messages: List of message dictionaries
# MAGIC             
# MAGIC         Yields:
# MAGIC             Response chunks from the LLM
# MAGIC         """
# MAGIC         with warnings.catch_warnings():
# MAGIC             warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
# MAGIC             for chunk in self.model_serving_client.chat.completions.create(
# MAGIC                 model=self.llm_endpoint,
# MAGIC                 messages=to_chat_completions_input(messages),
# MAGIC                 tools=self.get_tool_specs(),
# MAGIC                 stream=True,
# MAGIC             ):
# MAGIC                 chunk_dict = chunk.to_dict()
# MAGIC                 if len(chunk_dict.get("choices", [])) > 0:
# MAGIC                     yield chunk_dict
# MAGIC 
# MAGIC     def handle_tool_call(
# MAGIC         self,
# MAGIC         tool_call: dict[str, Any],
# MAGIC         messages: list[dict[str, Any]],
# MAGIC     ) -> ResponsesAgentStreamEvent:
# MAGIC         """
# MAGIC         Execute tool calls, add them to the running message history, 
# MAGIC         and return a ResponsesStreamEvent w/ tool output
# MAGIC         
# MAGIC         Args:
# MAGIC             tool_call: Tool call dictionary
# MAGIC             messages: Message history
# MAGIC             
# MAGIC         Returns:
# MAGIC             ResponsesAgentStreamEvent with tool output
# MAGIC         """
# MAGIC         args = json.loads(tool_call["arguments"])
# MAGIC         result = str(self.execute_tool(tool_name=tool_call["name"], args=args))
# MAGIC 
# MAGIC         tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
# MAGIC         messages.append(tool_call_output)
# MAGIC         return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)
# MAGIC 
# MAGIC     def call_and_run_tools(
# MAGIC         self,
# MAGIC         messages: list[dict[str, Any]],
# MAGIC         max_iter: int = 10,
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         """
# MAGIC         Main agent loop: call LLM, execute tools, repeat until done.
# MAGIC         
# MAGIC         Args:
# MAGIC             messages: Initial messages
# MAGIC             max_iter: Maximum iterations to prevent infinite loops
# MAGIC             
# MAGIC         Yields:
# MAGIC             ResponsesAgentStreamEvent objects
# MAGIC         """
# MAGIC         for _ in range(max_iter):
# MAGIC             last_msg = messages[-1]
# MAGIC             if last_msg.get("role", None) == "assistant":
# MAGIC                 return
# MAGIC             elif last_msg.get("type", None) == "function_call":
# MAGIC                 yield self.handle_tool_call(last_msg, messages)
# MAGIC             else:
# MAGIC                 yield from output_to_responses_items_stream(
# MAGIC                     chunks=self.call_llm(messages), aggregator=messages
# MAGIC                 )
# MAGIC 
# MAGIC         yield ResponsesAgentStreamEvent(
# MAGIC             type="response.output_item.done",
# MAGIC             item=self.create_text_output_item("Max iterations reached. Stopping.", str(uuid4())),
# MAGIC         )
# MAGIC 
# MAGIC     def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
# MAGIC         """
# MAGIC         Non-streaming prediction.
# MAGIC         
# MAGIC         Args:
# MAGIC             request: ResponsesAgentRequest
# MAGIC             
# MAGIC         Returns:
# MAGIC             ResponsesAgentResponse
# MAGIC         """
# MAGIC         outputs = [
# MAGIC             event.item
# MAGIC             for event in self.predict_stream(request)
# MAGIC             if event.type == "response.output_item.done"
# MAGIC         ]
# MAGIC         return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)
# MAGIC 
# MAGIC     def predict_stream(
# MAGIC         self, request: ResponsesAgentRequest
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         """
# MAGIC         Streaming prediction.
# MAGIC         
# MAGIC         Args:
# MAGIC             request: ResponsesAgentRequest
# MAGIC             
# MAGIC         Yields:
# MAGIC             ResponsesAgentStreamEvent objects
# MAGIC         """
# MAGIC         messages = to_chat_completions_input([i.model_dump() for i in request.input])
# MAGIC         if self.system_prompt:
# MAGIC             messages.insert(0, {"role": "system", "content": self.system_prompt})
# MAGIC         yield from self.call_and_run_tools(messages=messages)
# MAGIC 
# MAGIC 
# MAGIC def create_agent(
# MAGIC     agent_name: str,
# MAGIC     llm_endpoint: str,
# MAGIC     uc_tool_names: list[str],
# MAGIC     system_prompt: str
# MAGIC ) -> RiskBricksAgent:
# MAGIC     """
# MAGIC     Factory function to create a RiskBricks agent with UC tools.
# MAGIC     
# MAGIC     Args:
# MAGIC         agent_name: Name for the agent
# MAGIC         llm_endpoint: Databricks Foundation Model endpoint
# MAGIC         uc_tool_names: List of Unity Catalog function names
# MAGIC         system_prompt: System prompt for the agent
# MAGIC         
# MAGIC     Returns:
# MAGIC         Configured RiskBricksAgent
# MAGIC     """
# MAGIC     # Create UC toolkit
# MAGIC     uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC     uc_function_client = get_uc_function_client()
# MAGIC     
# MAGIC     # Create tool infos
# MAGIC     tool_infos = []
# MAGIC     for tool_spec in uc_toolkit.tools:
# MAGIC         tool_infos.append(create_tool_info(tool_spec, uc_function_client=uc_function_client))
# MAGIC     
# MAGIC     # Create and return agent
# MAGIC     agent = RiskBricksAgent(
# MAGIC         llm_endpoint=llm_endpoint,
# MAGIC         tools=tool_infos,
# MAGIC         system_prompt=system_prompt
# MAGIC     )
# MAGIC     
# MAGIC     return agent
# MAGIC 
# MAGIC 
# MAGIC # For MLflow logging
# MAGIC mlflow.openai.autolog()

# COMMAND ----------

print("✅ agent_base.py created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Create & Deploy Forecast Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Forecast Agent to File

# COMMAND ----------

# MAGIC %%writefile forecast_agent.py
# MAGIC import mlflow
# MAGIC from agent_base import create_agent
# MAGIC 
# MAGIC # Configuration
# MAGIC LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
# MAGIC 
# MAGIC UC_TOOL_NAMES = [
# MAGIC     "riskbricks.tools.get_latest_forecast",
# MAGIC     "riskbricks.tools.get_forecast_consensus",
# MAGIC     "riskbricks.tools.get_risk_metrics",
# MAGIC     "riskbricks.tools.get_company_info"
# MAGIC ]
# MAGIC 
# MAGIC SYSTEM_PROMPT = """You are a quantitative finance expert specializing in stock price forecasting.
# MAGIC You have access to multiple forecasting models (GBM, Ridge, Mean, News Event).
# MAGIC 
# MAGIC When asked about forecasts:
# MAGIC 1. Query forecast data using tools
# MAGIC 2. Analyze consensus across models
# MAGIC 3. Consider risk metrics and volatility
# MAGIC 4. Provide confidence intervals
# MAGIC 5. Explain methodology and discrepancies
# MAGIC 6. Give actionable insights
# MAGIC 
# MAGIC Always be transparent about model limitations."""
# MAGIC 
# MAGIC # Create agent
# MAGIC AGENT = create_agent(
# MAGIC     agent_name="forecast_agent",
# MAGIC     llm_endpoint=LLM_ENDPOINT,
# MAGIC     uc_tool_names=UC_TOOL_NAMES,
# MAGIC     system_prompt=SYSTEM_PROMPT
# MAGIC )
# MAGIC 
# MAGIC # Set for MLflow
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

print("✅ forecast_agent.py created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Forecast Agent Locally

# COMMAND ----------

# Import directly - both files are now in the working directory
from forecast_agent import AGENT as forecast_agent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentRequestItem

# Test prediction
test_input = ResponsesAgentRequest(
    input=[ResponsesAgentRequestItem(role="user", content="What is the forecast for AAPL?")]
)

print("🧪 Testing Forecast Agent...")
result = forecast_agent.predict(test_input)
print(f"✅ Test successful! Response: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log & Register Forecast Agent

# COMMAND ----------

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from pkg_resources import get_distribution

# Set registry to UC
mlflow.set_registry_uri("databricks-uc")

# Define resources for auth passthrough
resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT)]
for tool_name in FORECAST_TOOLS:
    resources.append(DatabricksFunction(function_name=tool_name))

# Input example
input_example = {
    "input": [
        {
            "role": "user",
            "content": "What is the forecast for NVDA?"
        }
    ]
}

# Log the agent
with mlflow.start_run(run_name="forecast_agent"):
    logged_agent_info = mlflow.pyfunc.log_model(
        name="forecast_agent",
        python_model="forecast_agent.py",
        code_paths=["agent_base.py"],
        input_example=input_example,
        pip_requirements=[
            "databricks-openai",
            "databricks-agents",
            "backoff",
            f"databricks-connect=={get_distribution('databricks-connect').version}",
        ],
        resources=resources,
    )

# Register to UC
forecast_model_name = f"{CATALOG}.{SCHEMA}.forecast_agent"
forecast_uc_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=forecast_model_name
)

print(f"✅ Forecast Agent registered: {forecast_model_name} v{forecast_uc_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy Forecast Agent

# COMMAND ----------

from databricks import agents

# Deploy with databricks-agents SDK
deployment_info = agents.deploy(
    forecast_model_name,
    forecast_uc_info.version,
    scale_to_zero=True
)

print(f"✅ Forecast Agent deployed!")
print(f"   Endpoint: {deployment_info.endpoint_name}")
print(f"   URL: {deployment_info.endpoint_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Create & Deploy Risk Agent

# COMMAND ----------

# MAGIC %%writefile risk_agent.py
# MAGIC import mlflow
# MAGIC from agent_base import create_agent
# MAGIC 
# MAGIC LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
# MAGIC 
# MAGIC UC_TOOL_NAMES = [
# MAGIC     "riskbricks.tools.get_risk_metrics",
# MAGIC     "riskbricks.tools.get_sector_risk_summary",
# MAGIC     "riskbricks.tools.get_company_info"
# MAGIC ]
# MAGIC 
# MAGIC SYSTEM_PROMPT = """You are a risk management expert specializing in portfolio risk analytics.
# MAGIC 
# MAGIC You calculate and interpret:
# MAGIC - Volatility (Historical, EWMA, forward-looking)
# MAGIC - Value at Risk (VaR) at 95% and 99%
# MAGIC - Expected Shortfall (ES)
# MAGIC - Beta and market sensitivity
# MAGIC - Maximum Drawdown
# MAGIC - Market Impact and liquidity
# MAGIC 
# MAGIC Always provide context and actionable recommendations."""
# MAGIC 
# MAGIC AGENT = create_agent(
# MAGIC     agent_name="risk_agent",
# MAGIC     llm_endpoint=LLM_ENDPOINT,
# MAGIC     uc_tool_names=UC_TOOL_NAMES,
# MAGIC     system_prompt=SYSTEM_PROMPT
# MAGIC )
# MAGIC 
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from risk_agent import AGENT as risk_agent

# Test
test_input = {"input": [{"role": "user", "content": "What is the risk profile of NVDA?"}]}
result = risk_agent.predict(test_input)
print(result)

# COMMAND ----------

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT)]
for tool_name in RISK_TOOLS:
    resources.append(DatabricksFunction(function_name=tool_name))

with mlflow.start_run(run_name="risk_agent"):
    logged_agent_info = mlflow.pyfunc.log_model(
        name="risk_agent",
        python_model="risk_agent.py",
        code_paths=["agent_base.py"],
        input_example={"input": [{"role": "user", "content": "Risk profile of TSLA?"}]},
        pip_requirements=["databricks-openai", "databricks-agents", "backoff"],
        resources=resources,
    )

risk_model_name = f"{CATALOG}.{SCHEMA}.risk_agent"
risk_uc_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=risk_model_name
)

print(f"✅ Risk Agent registered: {risk_model_name} v{risk_uc_info.version}")

# COMMAND ----------

from databricks import agents

deployment_info = agents.deploy(
    risk_model_name,
    risk_uc_info.version,
    scale_to_zero=True
)

print(f"✅ Risk Agent deployed!")
print(f"   Endpoint: {deployment_info.endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Create & Deploy Decision Agent

# COMMAND ----------

# MAGIC %%writefile decision_agent.py
# MAGIC import mlflow
# MAGIC from agent_base import create_agent
# MAGIC 
# MAGIC LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
# MAGIC 
# MAGIC UC_TOOL_NAMES = [
# MAGIC     "riskbricks.tools.get_decision_signal",
# MAGIC     "riskbricks.tools.get_latest_forecast",
# MAGIC     "riskbricks.tools.get_risk_metrics",
# MAGIC     "riskbricks.tools.get_earnings_surprise",
# MAGIC     "riskbricks.tools.get_analyst_ratings",
# MAGIC     "riskbricks.tools.get_company_info"
# MAGIC ]
# MAGIC 
# MAGIC SYSTEM_PROMPT = """You are an investment decision expert generating BUY/SELL/HOLD signals.
# MAGIC 
# MAGIC Decision framework:
# MAGIC - STRONG BUY: >3% return, low risk, positive catalysts
# MAGIC - BUY: >1% return, acceptable risk
# MAGIC - HOLD: -1% to +1% or mixed signals
# MAGIC - SELL: <-1% return
# MAGIC - STRONG SELL: <-3% return, high risk
# MAGIC 
# MAGIC Always explain reasoning, quantify confidence, and suggest position sizing."""
# MAGIC 
# MAGIC AGENT = create_agent(
# MAGIC     agent_name="decision_agent",
# MAGIC     llm_endpoint=LLM_ENDPOINT,
# MAGIC     uc_tool_names=UC_TOOL_NAMES,
# MAGIC     system_prompt=SYSTEM_PROMPT
# MAGIC )
# MAGIC 
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from decision_agent import AGENT as decision_agent

test_input = {"input": [{"role": "user", "content": "Should I buy MSFT?"}]}
result = decision_agent.predict(test_input)
print(result)

# COMMAND ----------

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT)]
for tool_name in DECISION_TOOLS:
    resources.append(DatabricksFunction(function_name=tool_name))

with mlflow.start_run(run_name="decision_agent"):
    logged_agent_info = mlflow.pyfunc.log_model(
        name="decision_agent",
        python_model="decision_agent.py",
        code_paths=["agent_base.py"],
        input_example={"input": [{"role": "user", "content": "Should I buy GOOGL?"}]},
        pip_requirements=["databricks-openai", "databricks-agents", "backoff"],
        resources=resources,
    )

decision_model_name = f"{CATALOG}.{SCHEMA}.decision_agent"
decision_uc_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=decision_model_name
)

print(f"✅ Decision Agent registered: {decision_model_name} v{decision_uc_info.version}")

# COMMAND ----------

from databricks import agents

deployment_info = agents.deploy(
    decision_model_name,
    decision_uc_info.version,
    scale_to_zero=True
)

print(f"✅ Decision Agent deployed!")
print(f"   Endpoint: {deployment_info.endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Create & Deploy Supervisor Agent

# COMMAND ----------

# MAGIC %%writefile supervisor_agent.py
# MAGIC import mlflow
# MAGIC from agent_base import create_agent
# MAGIC 
# MAGIC LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
# MAGIC 
# MAGIC UC_TOOL_NAMES = [
# MAGIC     "riskbricks.tools.get_latest_forecast",
# MAGIC     "riskbricks.tools.get_risk_metrics",
# MAGIC     "riskbricks.tools.get_decision_signal",
# MAGIC     "riskbricks.tools.get_top_opportunities",
# MAGIC     "riskbricks.tools.get_portfolio_summary",
# MAGIC     "riskbricks.tools.get_sector_risk_summary"
# MAGIC ]
# MAGIC 
# MAGIC SYSTEM_PROMPT = """You are the Chief Investment Officer coordinating specialist AI agents.
# MAGIC 
# MAGIC Your team:
# MAGIC - Forecast Agent: Price predictions
# MAGIC - Risk Agent: Risk analytics
# MAGIC - Decision Agent: BUY/SELL/HOLD signals
# MAGIC 
# MAGIC Your role:
# MAGIC - Coordinate agents for complex questions
# MAGIC - Synthesize multi-agent insights
# MAGIC - Provide portfolio-level analysis
# MAGIC - Generate executive summaries
# MAGIC 
# MAGIC Always delegate to specialists and add strategic overlay."""
# MAGIC 
# MAGIC AGENT = create_agent(
# MAGIC     agent_name="supervisor_agent",
# MAGIC     llm_endpoint=LLM_ENDPOINT,
# MAGIC     uc_tool_names=UC_TOOL_NAMES,
# MAGIC     system_prompt=SYSTEM_PROMPT
# MAGIC )
# MAGIC 
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from supervisor_agent import AGENT as supervisor_agent

test_input = {"input": [{"role": "user", "content": "What are the top 3 investment opportunities?"}]}
result = supervisor_agent.predict(test_input)
print(result)

# COMMAND ----------

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT)]
for tool_name in SUPERVISOR_TOOLS:
    resources.append(DatabricksFunction(function_name=tool_name))

with mlflow.start_run(run_name="supervisor_agent"):
    logged_agent_info = mlflow.pyfunc.log_model(
        name="supervisor_agent",
        python_model="supervisor_agent.py",
        code_paths=["agent_base.py"],
        input_example={"input": [{"role": "user", "content": "Portfolio analysis"}]},
        pip_requirements=["databricks-openai", "databricks-agents", "backoff"],
        resources=resources,
    )

supervisor_model_name = f"{CATALOG}.{SCHEMA}.supervisor"
supervisor_uc_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=supervisor_model_name
)

print(f"✅ Supervisor Agent registered: {supervisor_model_name} v{supervisor_uc_info.version}")

# COMMAND ----------

from databricks import agents

deployment_info = agents.deploy(
    supervisor_model_name,
    supervisor_uc_info.version,
    scale_to_zero=True
)

print(f"✅ Supervisor Agent deployed!")
print(f"   Endpoint: {deployment_info.endpoint_name}")
print(f"   URL: {deployment_info.endpoint_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Deployment Complete!

# COMMAND ----------

print("""
🎉 All 4 RiskBricks AI Agents Deployed Successfully!

✅ Agents using OFFICIAL Databricks pattern:
   - MLflow ResponsesAgent
   - databricks-openai.UCFunctionToolkit
   - databricks.agents.deploy()

✅ Registered in Unity Catalog:
   1. riskbricks.agents.forecast_agent
   2. riskbricks.agents.risk_agent
   3. riskbricks.agents.decision_agent
   4. riskbricks.agents.supervisor

✅ Deployed to Model Serving:
   - All endpoints running with scale-to-zero
   - Full auth passthrough configured
   - Tracing enabled via MLflow

Next steps:
1. Go to Machine Learning → Serving to view endpoints
2. Test agents with sample queries
3. Review MLflow traces for debugging
4. Run evaluation metrics (optional)
""")

# COMMAND ----------

dbutils.notebook.exit("success")
