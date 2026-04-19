"""RiskBricks Multi-Agent Supervisor v8 — Mosaic AI Agent Framework

Implements a Supervisor → Sub-Agent architecture using LangGraph.
The Supervisor routes user queries to 6 specialized agents, each with
their own tools and domain-specific system prompts.

Sub-agents:
  1. Risk Agent        — VaR, stress tests, portfolio risk metrics
  2. Price Target Agent    — Stock price predictions, evaluation
  3. Factor Agent      — Factor exposures, sector allocations
  4. Decision Agent    — Buy/hold/sell signals, macro context
  5. News Agent        — Financial news, RSS, GDELT events
  6. ML Direction Agent — Ensemble direction predictions

Usage:
  Logged via: mlflow.pyfunc.log_model(python_model="path/to/this_file.py", ...)
"""

import json
import os
import re
import operator
import functools
from typing import Optional, Generator, Literal, Annotated, Sequence, TypedDict

import mlflow
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.prebuilt import create_react_agent
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from mlflow.pyfunc import ChatAgent
import logging
from datetime import datetime, timezone

logger = logging.getLogger("riskbricks.agent")

from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

import logging
import time
import hashlib
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Structured Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("riskbricks.agent")

# ---------------------------------------------------------------------------
# Production Guardrails
# ---------------------------------------------------------------------------
MAX_INPUT_LENGTH = 2000          # chars — reject oversized prompts
MAX_GRAPH_RECURSION = 15         # prevent infinite supervisor loops
BLOCKED_PATTERNS = re.compile(   # basic prompt-injection detection
    r"(ignore previous|ignore above|system prompt|you are now|pretend you|"
    r"disregard|override|reveal your|show me your prompt|jailbreak)",
    re.IGNORECASE,
)
SENSITIVE_OUTPUT_PATTERNS = re.compile(  # redact if model leaks internals
    r"(api[_-]?key|password|secret|bearer\s+[a-zA-Z0-9])",
    re.IGNORECASE,
)

def validate_input(text: str) -> tuple[bool, str]:
    """Validate user input. Returns (is_valid, rejection_reason)."""
    if not text or not text.strip():
        return False, "Empty input"
    if len(text) > MAX_INPUT_LENGTH:
        return False, f"Input too long ({len(text)} chars, max {MAX_INPUT_LENGTH})"
    if BLOCKED_PATTERNS.search(text):
        logger.warning(f"Prompt injection attempt detected: {text[:100]}...")
        return False, "I can only answer questions about portfolio risk, forecasts, and market data."
    return True, ""

def sanitize_output(text: str) -> str:
    """Redact any accidentally leaked sensitive content."""
    return SENSITIVE_OUTPUT_PATTERNS.sub("[REDACTED]", text)

def _request_id(messages: list) -> str:
    """Generate a short deterministic request ID for audit correlation."""
    raw = json.dumps([str(m) for m in messages[-3:]], sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
CATALOG = os.getenv("RISKBRICKS_CATALOG", "riskbricks")
TOOLS_SCHEMA = f"{CATALOG}.agent_tools"

# ---------------------------------------------------------------------------
# Guardrails — Input Sanitization & Audit Logging
# ---------------------------------------------------------------------------
MAX_INPUT_LENGTH = 4000  # chars
BLOCKED_PATTERNS = [
    "ignore previous instructions",
    "ignore all previous",
    "disregard your instructions",
    "you are now",
    "system prompt",
    "reveal your prompt",
]

def sanitize_input(text: str) -> str:
    """Sanitize user input: length cap + prompt injection detection."""
    if not text or not isinstance(text, str):
        return ""
    text = text[:MAX_INPUT_LENGTH]
    text_lower = text.lower()
    for pattern in BLOCKED_PATTERNS:
        if pattern in text_lower:
            logger.warning(f"Blocked prompt injection attempt: {pattern!r}")
            return "I can only answer questions about portfolio risk, forecasts, and financial data."
    return text

def log_agent_request(user_message: str, response_text: str, agent_route: str, latency_ms: float):
    """Audit log every agent interaction via SQL Statement API (works in serving endpoints)."""
    try:
        import requests as _req
        host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        token = os.getenv("DATABRICKS_TOKEN", "")
        if not host or not token:
            return
        ts = datetime.now(timezone.utc).isoformat()
        um = user_message[:500].replace("'", "''")
        rt = response_text[:500].replace("'", "''")
        ar = agent_route.replace("'", "''")
        sql = (
            f"INSERT INTO {CATALOG}.gold.agent_audit_log "
            f"(timestamp, user_message, response_text, agent_route, latency_ms) "
            f"VALUES ('{ts}', '{um}', '{rt}', '{ar}', {latency_ms})"
        )
        _req.post(
            f"{host}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {token}"},
            json={"warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", ""), "statement": sql, "wait_timeout": "10s"},
            timeout=5,
        )
    except Exception:
        pass  # Best-effort — never fail the agent on audit logging

# ---------------------------------------------------------------------------
# UC Client + LLM
# ---------------------------------------------------------------------------
uc_client = DatabricksFunctionClient()
llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=4096)

# ---------------------------------------------------------------------------
# Tool Groups — one toolkit per sub-agent
# ---------------------------------------------------------------------------
def _get_tools(function_names):
    tk = UCFunctionToolkit(
        function_names=[f"{TOOLS_SCHEMA}.{fn}" for fn in function_names],
        client=uc_client,
    )
    return tk.tools

RISK_TOOLS        = _get_tools(["get_portfolio_risk_metrics", "get_stress_test_results"])
PRICE_TARGET_TOOLS    = _get_tools(["get_stock_forecast"])
FACTOR_TOOLS      = _get_tools(["get_factor_exposures", "get_sector_exposures"])
DECISION_TOOLS    = _get_tools(["get_decision_signal", "get_macro_context"])
NEWS_TOOLS        = _get_tools(["get_news_context", "get_portfolio_holdings"])
ML_DIRECTION_TOOLS = _get_tools(["get_ml_stock_forecast", "get_ml_market_overview"])

# ---------------------------------------------------------------------------
# Sub-Agent System Prompts
# ---------------------------------------------------------------------------
RISK_PROMPT = """You are the **Risk Agent** for RiskBricks.
You specialize in portfolio risk analysis using ONLY real data from your tools.

## MANDATORY RULES
- You MUST call get_portfolio_risk_metrics BEFORE stating any VaR, beta, or volatility numbers
- You MUST call get_stress_test_results when asked about stress scenarios or drawdowns
- NEVER invent or estimate risk numbers — only report what the tools return

## Tools
- get_portfolio_risk_metrics(manager_name) — Returns VaR (1d/10d at 95%), beta, volatility, AUM
- get_stress_test_results(manager_name) — Returns impact of stress scenarios (market crash, rate shock, etc.)

## Response Format — ALWAYS use markdown tables
For risk comparisons, present as:
| Manager | Risk Profile | AUM | Beta | Volatility | VaR 1-Day (95%) | VaR 10-Day (95%) |
|---------|-------------|-----|------|-----------|-----------------|------------------|

For stress tests:
| Manager | Scenario | Impact ($) | Impact (%) |
|---------|----------|-----------|-----------|

After the table, add 2-3 sentences of KEY INSIGHTS only (e.g. who has highest risk, which scenario is worst). Do NOT explain what VaR or beta means — the user is a finance professional."""

PRICE_TARGET_PROMPT = """You are the **Price Target Agent** for RiskBricks.
You specialize in stock price forecasting using ONLY real data from your tools.

## MANDATORY RULES
- You MUST call get_stock_forecast BEFORE stating any price prediction
- NEVER invent predicted prices or directions — only report what the tool returns

## Tools
- get_stock_forecast(symbol) — Returns ML-generated price predictions with confidence bands for 1d and 15d horizons

## Response Format — ALWAYS use markdown tables
| Horizon | Last Close | Predicted Price | Change | Direction | Confidence Low | Confidence High |
|---------|-----------|----------------|--------|-----------|---------------|----------------|

After the table, add 2-3 sentences: direction outlook, confidence band width, key risk. No disclaimers."""

FACTOR_PROMPT = """You are the **Factor Agent** for RiskBricks.
You specialize in factor exposure and sector allocation analysis using ONLY real data from your tools.

## MANDATORY RULES
- You MUST call your tools BEFORE stating any factor betas or sector weights
- NEVER invent factor exposures or sector allocations

## Tools
- get_factor_exposures(symbol) — Returns Fama-French factor betas (market, SMB, HML), alpha, annualized vol
- get_sector_exposures(manager_name) — Returns sector weight breakdown

## Response Format — ALWAYS use markdown tables
For factor exposures:
| Symbol | Market Beta | SMB Beta | HML Beta | Alpha | Ann. Volatility |
|--------|------------|----------|----------|-------|----------------|

For sector allocations:
| Manager | Sector | Weight |
|---------|--------|--------|

After the table, add 2-3 sentences of KEY INSIGHTS. No textbook explanations."""

DECISION_PROMPT = """You are the **Decision Agent** for RiskBricks.
You provide investment decision support using ONLY real data from your tools.

## MANDATORY RULES
- You MUST call get_decision_signal BEFORE giving any buy/hold/sell recommendation
- You MUST call get_macro_context when asked about economic conditions
- NEVER invent stock symbols, prices, or signals — if you don't have data, say so

## Tools
- get_decision_signal(symbol) — Returns BUY/HOLD/SELL with score, expected return, volatility, beta
- get_macro_context() — Returns Fed Funds rate, CPI, GDP, VIX, S&P 500 levels

## Response Format — ALWAYS use markdown tables
For decision signals:
| Symbol | Signal | Score | Expected Return | Volatility | Beta |
|--------|--------|-------|----------------|-----------|------|

For macro context:
| Indicator | Value | As Of | Units |
|-----------|-------|-------|-------|

After the table, add 2-3 sentences of KEY INSIGHTS (e.g. strongest buy signal, macro headwinds). No generic advice or disclaimers."""

NEWS_PROMPT = """You are the **News Agent** for RiskBricks.
You specialize in financial news analysis using ONLY real data from your tools.

## MANDATORY RULES
- You MUST call get_news_context BEFORE summarizing any news
- You MUST call get_portfolio_holdings when asked about a specific manager's news exposure
- NEVER invent headlines or news events

## Tools
- get_news_context(symbol, sector) — Returns recent news articles from RSS feeds
- get_portfolio_holdings(manager_name) — Returns holdings detail for a manager"""

ML_DIRECTION_PROMPT = """You are the **ML Direction Agent** for RiskBricks.
You provide stock direction predictions using a registered ML ensemble model
(LightGBM + RandomForest + GradientBoosting) trained on 17 features from 6 data sources.

## MANDATORY RULES
- You MUST call get_ml_stock_forecast BEFORE stating any ML prediction
- You MUST call get_ml_market_overview when asked about broad market direction or sector outlook
- NEVER invent predictions — only report what the tools return
- Present direction (UP/DOWN), confidence level, model agreement (how many of 3 models agree)
- Highlight key features: RSI (overbought/oversold), VIX (fear gauge), gap_pct, sentiment
- If confidence is LOW (<20%), recommend NOT trading
- If confidence is HIGH (>40%), highlight as a strong signal

## Model Info
- Accuracy: 70.3% overall, 76.7% on high-confidence trades
- Top feature: gap_pct (overnight gap) — #1 most important

## Tools
- get_ml_stock_forecast(symbol) — ML ensemble prediction for a stock (or 'all' for all 52)
- get_ml_market_overview() — Market sentiment, sector breakdown, high-confidence trade count

## Response Format
1. Direction + Confidence level (HIGH/MEDIUM/LOW)
2. Model agreement (e.g., "3/3 models agree UP")
3. Key signals (RSI, VIX, sentiment, gap)
4. Warnings if any
5. Disclaimer: "This is an ML model prediction, not financial advice."
"""

# ---------------------------------------------------------------------------
# Create Sub-Agents (each is a compiled LangGraph ReAct agent)
# ---------------------------------------------------------------------------
risk_agent        = create_react_agent(llm, RISK_TOOLS, prompt=RISK_PROMPT)
price_target_agent    = create_react_agent(llm, PRICE_TARGET_TOOLS, prompt=PRICE_TARGET_PROMPT)
factor_agent      = create_react_agent(llm, FACTOR_TOOLS, prompt=FACTOR_PROMPT)
decision_agent    = create_react_agent(llm, DECISION_TOOLS, prompt=DECISION_PROMPT)
news_agent        = create_react_agent(llm, NEWS_TOOLS, prompt=NEWS_PROMPT)
ml_direction_agent = create_react_agent(llm, ML_DIRECTION_TOOLS, prompt=ML_DIRECTION_PROMPT)

# ---------------------------------------------------------------------------
# Agent registry
# ---------------------------------------------------------------------------
AGENTS = {
    "risk_agent": risk_agent,
    "price_target_agent": price_target_agent,
    "factor_agent": factor_agent,
    "decision_agent": decision_agent,
    "news_agent": news_agent,
    "ml_direction_agent": ml_direction_agent,
}
AGENT_NAMES = list(AGENTS.keys())

# ---------------------------------------------------------------------------
# Supervisor Prompt
# ---------------------------------------------------------------------------
SUPERVISOR_PROMPT = f"""You are the **RiskBricks Supervisor Agent**. You route user questions
to specialized sub-agents. You MUST respond with ONLY a JSON object and nothing else.

## Available Agents
- **risk_agent** — Portfolio risk metrics (VaR, beta, volatility), stress test impacts
- **price_target_agent** — Price target predictions at 1d/15d horizons with confidence bands
- **ml_direction_agent** — ML ensemble UP/DOWN predictions with model agreement scores
- **factor_agent** — Fama-French factor exposures (market/SMB/HML), sector allocations
- **decision_agent** — Buy/hold/sell decision signals, macro economic context
- **news_agent** — Recent financial news headlines, RSS feeds, portfolio holdings

## Available Managers
- **Sarah Russel** — Conservative (low beta, capital preservation)
- **Rena Tang** — Balanced (growth & income)
- **Mohit Arora** — Aggressive (high-growth tech)

## Routing Rules
1. Risk questions (VaR, stress tests, drawdown) → risk_agent
2. Price target questions (predicted price, confidence bands) → price_target_agent
2b. ML direction predictions (UP/DOWN, model confidence, market overview) → ml_direction_agent
3. Factor/sector analysis → factor_agent
4. Buy/sell recommendations, macro outlook → decision_agent
5. News, headlines, market events → news_agent
7. For multi-part questions, route to ONE agent at a time.
8. After all sub-agents have responded, synthesize and output {{"next": "FINISH"}}

## CRITICAL OUTPUT FORMAT
Your response MUST be ONLY a valid JSON object. No markdown, no explanation.
- To route: {{"next": "risk_agent"}}
- To finish: {{"next": "FINISH"}}
- Include context: {{"next": "decision_agent", "context": "Get signals for Mohit Arora"}}

When all sub-agents have responded, output {"next": "FINISH"} — do NOT summarize or rephrase the sub-agent response.
"""

# ---------------------------------------------------------------------------
# Multi-Agent Graph
# ---------------------------------------------------------------------------
class SupervisorState(MessagesState):
    next: str

_NEXT_RE = re.compile(r'\{\s*"next"\s*:\s*"([^"]+)"[^}]*\}')

def supervisor_node(state: SupervisorState) -> dict:
    messages = state["messages"]
    response = llm.invoke([SystemMessage(content=SUPERVISOR_PROMPT)] + messages)
    content = response.content.strip()
    next_agent = "FINISH"
    match = _NEXT_RE.search(content)
    if match:
        candidate = match.group(1)
        if candidate in AGENT_NAMES or candidate == "FINISH":
            next_agent = candidate
    if next_agent not in AGENT_NAMES and next_agent != "FINISH":
        next_agent = "FINISH"
    return {"messages": [response], "next": next_agent}

def agent_node(state: SupervisorState, agent_name: str) -> dict:
    agent = AGENTS[agent_name]
    n_input = len(state["messages"])
    result = agent.invoke({"messages": state["messages"]})
    agent_messages = result.get("messages", [])
    # CRITICAL: Only take NEW messages generated by the sub-agent,
    # not the input history that gets echoed back.
    new_messages = agent_messages[n_input:]
    output_messages = []
    for msg in new_messages:
        if hasattr(msg, "content") and msg.content and hasattr(msg, "type") and msg.type == "ai":
            output_messages.append(AIMessage(content=f"[{agent_name}]: {msg.content}"))
    if not output_messages and new_messages:
        last = new_messages[-1]
        output_messages = [AIMessage(content=f"[{agent_name}]: {last.content if hasattr(last, 'content') else str(last)}")]
    return {"messages": output_messages}

def route_next(state: SupervisorState) -> str:
    next_val = state.get("next", "FINISH")
    return next_val if next_val in AGENT_NAMES else "FINISH"

builder = StateGraph(SupervisorState)
builder.add_node("supervisor", supervisor_node)
for name in AGENT_NAMES:
    builder.add_node(name, functools.partial(agent_node, agent_name=name))
builder.add_edge(START, "supervisor")
builder.add_conditional_edges(
    "supervisor", route_next,
    {name: name for name in AGENT_NAMES} | {"FINISH": END},
)
for name in AGENT_NAMES:
    builder.add_edge(name, "supervisor")

multi_agent_graph = builder.compile()
multi_agent_graph.recursion_limit = 25  # Safety: prevent infinite agent loops
# Apply recursion limit for production safety
multi_agent_graph.recursion_limit = MAX_GRAPH_RECURSION

# ---------------------------------------------------------------------------
# ChatAgent Wrapper
# ---------------------------------------------------------------------------
_TYPE_TO_ROLE = {"ai": "assistant", "human": "user", "tool": "tool", "system": "system"}

def _convert_lc_tool_calls(lc_tool_calls):
    return [{
        "id": tc.get("id", ""),
        "type": "function",
        "function": {"name": tc.get("name", ""), "arguments": json.dumps(tc.get("args", {}))},
    } for tc in lc_tool_calls]

def _lc_msg_to_dict(lc_msg):
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

class RiskBricksSupervisor(ChatAgent):
    """Multi-agent supervisor wrapped in MLflow ChatAgent for serving.

    Production features:
      - Input validation & prompt injection detection
      - Output sanitization (redact leaked secrets)
      - Structured audit logging (request/response/latency)
      - Recursion-limited graph execution
    """

    def __init__(self, graph):
        self.graph = graph

    # -- Audit helper (logs to stdout; route to Delta via log sink) --------
    def _audit_log(self, request_id: str, action: str, **kwargs):
        """Structured audit entry — can be captured by Databricks log delivery."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id,
            "action": action,
            **kwargs,
        }
        logger.info(json.dumps(entry))

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict] = None,
    ) -> ChatAgentResponse:
        t0 = time.time()
        request_id = _request_id(messages)

        # --- Input guardrail ---
        last_msg = messages[-1].content if messages else ""
        is_valid, reason = validate_input(last_msg)
        if not is_valid:
            self._audit_log(request_id, "REJECTED", reason=reason)
            return ChatAgentResponse(
                messages=[ChatAgentMessage(role="assistant", content=reason)]
            )

        self._audit_log(request_id, "REQUEST", user_message=last_msg[:200])

        try:
            request = {"messages": self._convert_messages_to_dict(messages)}
            output_messages = []
            for event in self.graph.stream(request, stream_mode="updates"):
                for node_data in event.values():
                    output_messages.extend(
                        ChatAgentMessage(**_lc_msg_to_dict(msg))
                        for msg in node_data.get("messages", [])
                    )

            # --- Output guardrail ---
            for msg in output_messages:
                if msg.content:
                    msg.content = sanitize_output(msg.content)

            latency_ms = int((time.time() - t0) * 1000)
            self._audit_log(
                request_id, "RESPONSE",
                latency_ms=latency_ms,
                num_messages=len(output_messages),
            )

            # Delta audit log (REST-based, works in serving)
            _user_msg = last_msg[:500] if last_msg else ""
            _resp_text = output_messages[-1].content if output_messages else ""
            log_agent_request(_user_msg, _resp_text, "multi", latency_ms)

            return ChatAgentResponse(messages=output_messages)

        except Exception as e:
            latency_ms = int((time.time() - t0) * 1000)
            self._audit_log(request_id, "ERROR", error=str(e)[:500], latency_ms=latency_ms)
            logger.exception(f"Agent error for request {request_id}")
            return ChatAgentResponse(
                messages=[ChatAgentMessage(
                    role="assistant",
                    content="I encountered an error processing your request. Please try again.",
                )]
            )

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        t0 = time.time()
        request_id = _request_id(messages)

        # --- Input guardrail ---
        last_msg = messages[-1].content if messages else ""
        is_valid, reason = validate_input(last_msg)
        if not is_valid:
            self._audit_log(request_id, "REJECTED", reason=reason)
            yield ChatAgentChunk(
                delta=ChatAgentMessage(role="assistant", content=reason)
            )
            return

        self._audit_log(request_id, "REQUEST_STREAM", user_message=last_msg[:200])

        try:
            request = {"messages": self._convert_messages_to_dict(messages)}
            chunk_count = 0
            for event in self.graph.stream(request, stream_mode="updates"):
                for node_data in event.values():
                    for msg in node_data.get("messages", []):
                        agent_msg = ChatAgentMessage(**_lc_msg_to_dict(msg))
                        if agent_msg.content:
                            agent_msg.content = sanitize_output(agent_msg.content)
                        yield ChatAgentChunk(delta=agent_msg)
                        chunk_count += 1

            latency_ms = int((time.time() - t0) * 1000)
            self._audit_log(
                request_id, "RESPONSE_STREAM",
                latency_ms=latency_ms,
                chunks=chunk_count,
            )
        except Exception as e:
            latency_ms = int((time.time() - t0) * 1000)
            self._audit_log(request_id, "ERROR", error=str(e)[:500], latency_ms=latency_ms)
            logger.exception(f"Stream error for request {request_id}")
            yield ChatAgentChunk(
                delta=ChatAgentMessage(
                    role="assistant",
                    content="I encountered an error processing your request. Please try again.",
                )
            )

# ---------------------------------------------------------------------------
# Register with MLflow
# ---------------------------------------------------------------------------
mlflow.models.set_model(RiskBricksSupervisor(multi_agent_graph))
