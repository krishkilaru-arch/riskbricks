"""RiskBricks Agent — Mosaic AI Agent Framework

This module defines the RiskBricks financial risk analysis agent using
LangGraph + ChatDatabricks + UC function tools, wrapped in an MLflow
ChatAgent for proper streaming serialization.

Usage:
  - Logged via: mlflow.pyfunc.log_model(python_model="path/to/this_file.py", ...)
  - Supports both streaming and non-streaming invocation
"""

import json
import mlflow
from typing import Optional, Generator
from langgraph.prebuilt import create_react_agent
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
CATALOG = "riskbricks"
TOOLS_SCHEMA = f"{CATALOG}.agent_tools"

# ---------------------------------------------------------------------------
# UC Function Client — required for model serving containers
# ---------------------------------------------------------------------------
uc_client = DatabricksFunctionClient()

# ---------------------------------------------------------------------------
# Tools — UC functions registered in riskbricks.agent_tools
# ---------------------------------------------------------------------------
toolkit = UCFunctionToolkit(
    function_names=[
        f"{TOOLS_SCHEMA}.get_portfolio_risk_metrics",
        f"{TOOLS_SCHEMA}.get_stress_test_results",
        f"{TOOLS_SCHEMA}.get_portfolio_holdings",
        f"{TOOLS_SCHEMA}.get_sector_exposures",
        f"{TOOLS_SCHEMA}.get_macro_context",
        f"{TOOLS_SCHEMA}.get_stock_forecast",
        f"{TOOLS_SCHEMA}.get_decision_signal",
        f"{TOOLS_SCHEMA}.get_factor_exposures",
    ],
    client=uc_client,
)
tools = toolkit.tools

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
llm = ChatDatabricks(
    endpoint=LLM_ENDPOINT,
    temperature=0.1,
    max_tokens=4096,
)

# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are **RiskBricks**, an expert financial risk analysis assistant for a multi-manager equity portfolio platform.

## Your Role
You help portfolio managers and risk officers understand portfolio risk, analyze holdings, interpret stress tests, review stock forecasts, and make data-driven investment decisions.

## Available Managers
- **Sarah Russel** — Conservative, dividend-focused (low beta)
- **Rena Tang** — Balanced growth, moderate risk
- **Mohit Arora** — Aggressive growth, high-tech concentration

## Tools at Your Disposal
1. **get_portfolio_risk_metrics** — VaR (95%), beta, volatility, AUM for any manager
2. **get_stress_test_results** — Impact of Market Crash, Tech Drawdown, Rate Spike, Recession
3. **get_portfolio_holdings** — Top positions with weight, P&L, risk stats
4. **get_sector_exposures** — Sector allocation breakdown
5. **get_macro_context** — Fed Funds Rate, CPI, GDP, VIX, S&P 500
6. **get_stock_forecast** — Price forecasts at 1d/5d/15d horizons
7. **get_decision_signal** — Buy/Hold/Sell signals with composite scores
8. **get_factor_exposures** — Fama-French market/SMB/HML factor betas

## Response Guidelines
- Always use tools to fetch real data — never fabricate numbers
- Present dollar amounts with proper formatting ($X.XM or $X.XB)
- Express risk metrics clearly: "VaR of $X means the portfolio could lose up to $X in a single day at 95% confidence"
- When comparing managers, use tables for clarity
- Flag specific risks: concentration, high beta, drawdown exposure
- End with actionable recommendations when appropriate
- Be concise but thorough — a busy portfolio manager is your audience
"""

# ---------------------------------------------------------------------------
# Agent (LangGraph ReAct pattern)
# ---------------------------------------------------------------------------
langgraph_agent = create_react_agent(
    model=llm,
    tools=tools,
    prompt=SYSTEM_PROMPT,
)


# ---------------------------------------------------------------------------
# Helper: convert LangChain message objects to ChatAgentMessage-compatible dicts
# ---------------------------------------------------------------------------
_TYPE_TO_ROLE = {"ai": "assistant", "human": "user", "tool": "tool", "system": "system"}


def _convert_lc_tool_calls(lc_tool_calls):
    """Convert LangChain tool_calls to MLflow ToolCall format.

    LangChain: {name, args (dict), id, type: "tool_call"}
    MLflow:    {id, type: "function", function: {name, arguments (JSON str)}}
    """
    converted = []
    for tc in lc_tool_calls:
        converted.append({
            "id": tc.get("id", ""),
            "type": "function",
            "function": {
                "name": tc.get("name", ""),
                "arguments": json.dumps(tc.get("args", {})),
            },
        })
    return converted


def _lc_msg_to_dict(lc_msg):
    """Convert a LangChain BaseMessage to a dict suitable for ChatAgentMessage(**d)."""
    d = lc_msg.model_dump()
    role = _TYPE_TO_ROLE.get(d.get("type", ""), "assistant")
    result = {"role": role, "content": d.get("content", "") or "", "id": d.get("id")}
    if d.get("name"):
        result["name"] = d["name"]
    if d.get("tool_calls"):
        result["tool_calls"] = _convert_lc_tool_calls(d["tool_calls"])
    if d.get("tool_call_id"):
        result["tool_call_id"] = d["tool_call_id"]
    return result


# ---------------------------------------------------------------------------
# ChatAgent wrapper — fixes streaming serialization
# ---------------------------------------------------------------------------
class LangGraphChatAgent(ChatAgent):
    """Wraps the LangGraph agent in MLflow's ChatAgent interface
    so that both predict() and predict_stream() return properly
    serializable ChatAgentMessage / ChatAgentChunk objects."""

    def __init__(self, agent):
        self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict] = None,
    ) -> ChatAgentResponse:
        request = {"messages": self._convert_messages_to_dict(messages)}
        output_messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                output_messages.extend(
                    ChatAgentMessage(**_lc_msg_to_dict(msg))
                    for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=output_messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(delta=ChatAgentMessage(**_lc_msg_to_dict(msg)))
                    for msg in node_data.get("messages", [])
                )


# Register the wrapped agent with MLflow
mlflow.models.set_model(LangGraphChatAgent(langgraph_agent))
