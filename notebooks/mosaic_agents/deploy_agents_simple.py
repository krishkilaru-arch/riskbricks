# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Deploy RiskBricks AI Agents - Simple Inline Approach
# MAGIC 
# MAGIC This notebook uses the OFFICIAL Databricks ResponsesAgent pattern with inline definitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Packages

# COMMAND ----------

%pip install -U -qqq databricks-agents databricks-sdk databricks-openai backoff mlflow unitycatalog-ai
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔨 Define Agent Base Class (Inline)

# COMMAND ----------

import json
from typing import Any, Callable, Generator, Optional
from uuid import uuid4
import warnings

import mlflow
import openai
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from openai import OpenAI
from pydantic import BaseModel
from unitycatalog.ai.core.base import get_uc_function_client


class ToolInfo(BaseModel):
    """Tool specification for the agent."""
    name: str
    spec: dict
    exec_fn: Callable
    
    class Config:
        arbitrary_types_allowed = True


def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None, uc_function_client=None):
    """Create a ToolInfo object from a tool specification."""
    tool_spec["function"].pop("strict", None)
    tool_name = tool_spec["function"]["name"]
    udf_name = tool_name.replace("__", ".")

    def exec_fn(**kwargs):
        function_result = uc_function_client.execute_function(udf_name, kwargs)
        if function_result.error is not None:
            return function_result.error
        else:
            return function_result.value
    
    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


class RiskBricksAgent(ResponsesAgent):
    """RiskBricks Agent using official Databricks ResponsesAgent pattern."""

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo], system_prompt: str = ""):
        self.llm_endpoint = llm_endpoint
        self.system_prompt = system_prompt
        self.workspace_client = WorkspaceClient()
        self.model_serving_client: OpenAI = (
            self.workspace_client.serving_endpoints.get_open_ai_client()
        )
        self._tools_dict = {tool.name: tool for tool in tools}

    def get_tool_specs(self) -> list[dict]:
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        return self._tools_dict[tool_name].exec_fn(**args)

    def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
            for chunk in self.model_serving_client.chat.completions.create(
                model=self.llm_endpoint,
                messages=to_chat_completions_input(messages),
                tools=self.get_tool_specs(),
                stream=True,
            ):
                chunk_dict = chunk.to_dict()
                if len(chunk_dict.get("choices", [])) > 0:
                    yield chunk_dict

    def handle_tool_call(
        self,
        tool_call: dict[str, Any],
        messages: list[dict[str, Any]],
    ) -> ResponsesAgentStreamEvent:
        args = json.loads(tool_call["arguments"])
        result = str(self.execute_tool(tool_name=tool_call["name"], args=args))
        tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
        messages.append(tool_call_output)
        return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)

    def call_and_run_tools(
        self,
        messages: list[dict[str, Any]],
        max_iter: int = 10,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        for _ in range(max_iter):
            last_msg = messages[-1]
            if last_msg.get("role", None) == "assistant":
                return
            elif last_msg.get("type", None) == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )

        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item("Max iterations reached. Stopping.", str(uuid4())),
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([i.model_dump() for i in request.input])
        if self.system_prompt:
            messages.insert(0, {"role": "system", "content": self.system_prompt})
        yield from self.call_and_run_tools(messages=messages)


def create_agent(
    agent_name: str,
    llm_endpoint: str,
    uc_tool_names: list[str],
    system_prompt: str
) -> RiskBricksAgent:
    """Factory function to create a RiskBricks agent."""
    uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
    uc_function_client = get_uc_function_client()
    
    tool_infos = []
    for tool_spec in uc_toolkit.tools:
        tool_infos.append(create_tool_info(tool_spec, uc_function_client=uc_function_client))
    
    agent = RiskBricksAgent(
        llm_endpoint=llm_endpoint,
        tools=tool_infos,
        system_prompt=system_prompt
    )
    
    return agent


mlflow.openai.autolog()
print("✅ Agent base classes defined successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

CATALOG = "riskbricks"
SCHEMA = "agents"
TOOLS_SCHEMA = "tools"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

FORECAST_TOOLS = [
    f"{CATALOG}.{TOOLS_SCHEMA}.get_latest_forecast",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_forecast_consensus",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_risk_metrics",
    f"{CATALOG}.{TOOLS_SCHEMA}.get_company_info"
]

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

print("✅ Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Create Forecast Agent

# COMMAND ----------

# Create the agent
forecast_agent = create_agent(
    agent_name="forecast_agent",
    llm_endpoint=LLM_ENDPOINT,
    uc_tool_names=FORECAST_TOOLS,
    system_prompt=FORECAST_PROMPT
)

print("✅ Forecast agent created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Forecast Agent Locally

# COMMAND ----------

# Simple test without the fancy types (for compatibility)
print("🧪 Testing Forecast Agent...")
print("Note: Skipping local test - will test after deployment via endpoint")
print("✅ Agent created successfully, ready to deploy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Save Agent Code to File (Code-based Logging)

# COMMAND ----------

# Write forecast agent to a Python file for code-based logging
forecast_agent_code = f"""
import mlflow
from agent_base_inline import create_agent

# Configuration
LLM_ENDPOINT = "{LLM_ENDPOINT}"

UC_TOOL_NAMES = {FORECAST_TOOLS}

SYSTEM_PROMPT = '''{FORECAST_PROMPT}'''

# Create and set agent
AGENT = create_agent(
    agent_name="forecast_agent",
    llm_endpoint=LLM_ENDPOINT,
    uc_tool_names=UC_TOOL_NAMES,
    system_prompt=SYSTEM_PROMPT
)

mlflow.models.set_model(AGENT)
"""

with open("forecast_agent_model.py", "w") as f:
    f.write(forecast_agent_code)

# Save agent_base code to file too
import inspect
agent_base_code = f"""
{inspect.getsource(ToolInfo)}
{inspect.getsource(create_tool_info)}
{inspect.getsource(RiskBricksAgent)}
{inspect.getsource(create_agent)}

mlflow.openai.autolog()
"""

with open("agent_base_inline.py", "w") as f:
    f.write(agent_base_code)

print("✅ Agent code saved to files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Log & Register to UC

# COMMAND ----------

from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from databricks import agents

mlflow.set_registry_uri("databricks-uc")

forecast_model_name = f"{CATALOG}.{SCHEMA}.forecast_agent"

# Define resources for auth passthrough
resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT)]
for tool_name in FORECAST_TOOLS:
    resources.append(DatabricksFunction(function_name=tool_name))

print(f"📝 Logging agent with code-based approach...")

# Use code-based logging (official training pattern)
with mlflow.start_run(run_name="forecast_agent"):
    logged_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="forecast_agent_model.py",
        code_paths=["agent_base_inline.py"],
        pip_requirements=[
            "databricks-openai",
            "databricks-agents", 
            "backoff",
            "unitycatalog-ai",
        ],
        resources=resources,
    )

# Register to UC
uc_info = mlflow.register_model(
    model_uri=logged_info.model_uri,
    name=forecast_model_name
)

print(f"✅ Agent registered: {forecast_model_name} v{uc_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploy Forecast Agent

# COMMAND ----------

print(f"🚀 Deploying {forecast_model_name} v{uc_info.version}...")

# Deploy using databricks.agents SDK
deployment = agents.deploy(
    model_name=forecast_model_name,
    model_version=uc_info.version,
    scale_to_zero=True
)

print(f"✅ Forecast Agent deployed!")
print(f"   Endpoint: {deployment.endpoint_name}")
print(f"   Model: {forecast_model_name} v{uc_info.version}")

# COMMAND ----------

print("""
🎉 Forecast Agent Deployment Complete!

Next: Create and deploy the other 3 agents (Risk, Decision, Supervisor) 
using the same pattern above.
""")

# COMMAND ----------

dbutils.notebook.exit("success")
