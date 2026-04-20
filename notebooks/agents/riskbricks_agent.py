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
import uuid
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
MAX_GRAPH_RECURSION = 6          # max: supervisor→agent1→sup→agent2→sup→FINISH
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
# Configuration (self-contained — no workspace-only imports for serving)
# ---------------------------------------------------------------------------
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
CATALOG = os.getenv("RISKBRICKS_CATALOG", "riskbricks")
TOOLS_SCHEMA = f"{CATALOG}.agent_tools"

# ---------------------------------------------------------------------------
# Guardrails — Input Sanitization & Audit Logging
# ---------------------------------------------------------------------------
_SANITIZE_MAX_LENGTH = 4000  # chars
_BLOCKED_PHRASES = [
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
    text = text[:_SANITIZE_MAX_LENGTH]
    text_lower = text.lower()
    for pattern in _BLOCKED_PHRASES:
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
llm = ChatDatabricks(
    endpoint=LLM_ENDPOINT,
    temperature=0.1,
    max_tokens=1024,
)

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

After the table, add 2-3 sentences of KEY INSIGHTS only (e.g. who has highest risk, which scenario is worst). Do NOT explain what VaR or beta means — the user is a finance professional.

## Formatting
- Format currency as plain $123.45 — NEVER use backslash-escaped \$ signs."""

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

After the table, add 2-3 sentences: direction outlook, confidence band width, key risk. No disclaimers.

## Formatting
- Format currency as plain $123.45 — NEVER use backslash-escaped \$ signs."""

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
You provide investment decision signals using ONLY real data from your tools.

## MANDATORY RULES
- You MUST call get_decision_signal BEFORE giving any recommendation
- You MUST call get_macro_context when asked about economic conditions
- NEVER invent data — only report what the tools return

## CRITICAL OUTPUT RULE
Your FIRST sentence MUST state the signal using this EXACT pattern:
"The signal for [SYMBOL] is **[BUY/HOLD/SELL]** with a score of [X.XX]."

For multiple stocks, use this pattern:
"Top BUY signals: DLR (score 14.56), SPY (score 10.20), SLV (score 8.95)..."

## EXAMPLES (follow this format exactly):

Example 1 — Single stock:
The signal for NVDA is **BUY** with a score of 6.01 and expected return of 6.01%. Beta is 1.7.

Example 2 — Single stock with multiple signals:
The signal for AAPL is **HOLD** with a score of 1.06. A secondary BUY signal exists with score 3.83. Beta is 1.2, volatility is 1.42%.

Example 3 — All buy signals:
Top BUY signals: DLR (score 14.56), SPY (score 10.20), SLV (score 8.95), COF (score 8.07), NVDA (score 6.01).

## Tools
- get_decision_signal(symbol) — Returns BUY/HOLD/SELL with score, expected return, volatility, beta. Use "all" for top BUY signals.
- get_macro_context() — Returns Fed Funds rate, CPI, GDP, VIX, S&P 500 levels

## Response Format — ALWAYS use markdown tables
For decision signals:
| Symbol | Signal | Score | Expected Return | Volatility | Beta |
|--------|--------|-------|----------------|-----------|------|

For macro context:
| Indicator | Value | As Of | Units |
|-----------|-------|-------|-------|

After the table, your FIRST sentence MUST name the signal (BUY, HOLD, or SELL). Then add 1-2 sentences of insight.

## Formatting
- Format currency as plain $123.45 — NEVER use backslash-escaped \$ signs."""

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

After the table, add 2-3 sentences: direction outlook, confidence band width, key risk. No disclaimers.

## Formatting
- Format currency as plain $123.45 — NEVER use backslash-escaped \$ signs."""

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

## CRITICAL — ALWAYS ECHO THE SIGNAL KEYWORD
- Your response text MUST contain the exact word BUY, HOLD, or SELL (uppercase) for every stock discussed
- Start your summary sentence with the signal: "The signal for NVDA is **BUY** with a score of 6.01."
- For multi-stock queries, list each symbol with its signal: "DLR: **BUY** (14.56), SPY: **BUY** (10.20)..."
- NEVER omit the signal word — it is the most important piece of information

## Tools
- get_decision_signal(symbol) — Returns BUY/HOLD/SELL with score, expected return, volatility, beta. Use "all" for top BUY signals.
- get_macro_context() — Returns Fed Funds rate, CPI, GDP, VIX, S&P 500 levels

## Response Format — ALWAYS use markdown tables
For decision signals:
| Symbol | Signal | Score | Expected Return | Volatility | Beta |
|--------|--------|-------|----------------|-----------|------|

For macro context:
| Indicator | Value | As Of | Units |
|-----------|-------|-------|-------|

After the table, write a summary that:
1. Starts with "The signal for [SYMBOL] is **BUY/HOLD/SELL**" (MANDATORY — always use the exact signal word)
2. Mentions the conviction score
3. Adds 1-2 sentences of insight (e.g. strongest buy, risk factors)
No generic advice or disclaimers. Keep it under 4 sentences.

## Formatting
- Format currency as plain $123.45 — NEVER use backslash-escaped \$ signs."""

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

## CRITICAL — ALWAYS STATE THE DIRECTION
- Your response MUST contain the word "UP" or "DOWN" (uppercase) for the predicted direction
- Start your response with: "The ML forecast for [SYMBOL] is **UP/DOWN**"
- Always mention the direction, confidence level, and model agreement in the first sentence
- Example: "The ML forecast for NVDA is **UP** with MEDIUM confidence (0.52 probability, 2/3 models agree)."

## Model Info
- Accuracy: 70.3% overall, 76.7% on high-confidence trades
- Top feature: gap_pct (overnight gap) — #1 most important

## Tools
- get_ml_stock_forecast(symbol) — ML ensemble prediction for a stock (or 'all' for all 52)
- get_ml_market_overview() — Market sentiment, sector breakdown, high-confidence trade count

## Response Format
1. First sentence: "The ML forecast for [SYMBOL] is **UP/DOWN** with [HIGH/MEDIUM/LOW] confidence ([probability], [N/3] models agree)."
2. Key signals: RSI (overbought >70 / oversold <30), VIX (fear gauge), gap_pct, AI sentiment
3. If confidence LOW (<20%): "Low confidence — not recommended for trading."
4. If confidence HIGH (>40%): "High confidence signal — strong conviction."
5. Brief disclaimer: "This is an ML model prediction, not financial advice."
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

When all sub-agents have responded, output {{"next": "FINISH"}} — do NOT summarize or rephrase the sub-agent response.
"""


# ---------------------------------------------------------------------------
# Output post-processing — dedup repetitive LLM output
# ---------------------------------------------------------------------------
_AGENT_TAG_RE = re.compile(r"^\[(\w+)\]:\s*")
_ROUTING_JSON_RE = re.compile(r'^\s*\{\s*"next"\s*:.*\}\s*$', re.MULTILINE)

def _dedup_text(text: str) -> str:
    """Detect and truncate repetitive LLM output.

    Splits on 'The final answer is:' (the most common loop trigger for Llama)
    and keeps only the first occurrence. Also handles sentence-level repeats.
    """
    if not text:
        return text
    # Strategy 1: Split on "The final answer is:" — keep first segment
    marker = "The final answer is:"
    if text.count(marker) > 1:
        parts = text.split(marker)
        # Keep intro + first "final answer" block
        text = parts[0] + marker + parts[1]
        text = text.rstrip()

    # Strategy 2: Sentence-level dedup — if ≥3 consecutive identical sentences
    sentences = re.split(r'(?<=[.!?])\s+', text)
    if len(sentences) > 6:
        deduped = [sentences[0]]
        repeat_count = 0
        for i in range(1, len(sentences)):
            if sentences[i] == sentences[i - 1]:
                repeat_count += 1
                if repeat_count >= 2:
                    continue  # skip 3rd+ consecutive repeat
            else:
                repeat_count = 0
            deduped.append(sentences[i])
        text = " ".join(deduped)

    return text.strip()


def _extract_final_response(output_messages: list) -> list:
    """From all graph messages, extract only the meaningful sub-agent responses.

    Filters out:
      - Supervisor routing JSON messages ({"next": "..."})
      - Tool call / tool response messages
      - Duplicate content from repeated agent invocations
    """
    agent_responses = []
    seen_content = set()

    for msg in output_messages:
        if not msg.content:
            continue
        # Skip routing JSON — both pure JSON and hybrid text+JSON messages
        if _ROUTING_JSON_RE.match(msg.content.strip()):
            continue
        # FIX: Also catch hybrid messages like "The beta is 1.7... {"next": "FINISH"}"
        # These are supervisor summaries with routing JSON appended — always noise.
        if '"next"' in msg.content and re.search(r'\{\s*"next"\s*:', msg.content):
            continue
        # Skip tool messages
        if msg.role == "tool":
            continue
        # Skip very short messages (likely routing noise)
        if len(msg.content.strip()) < 30:
            continue

        # Dedup: use first 200 chars as fingerprint
        fingerprint = msg.content.strip()[:200]
        if fingerprint in seen_content:
            continue
        seen_content.add(fingerprint)

        # Apply repetition dedup to the content
        msg.content = _dedup_text(msg.content)
        agent_responses.append(msg)

    return agent_responses

# ---------------------------------------------------------------------------
# Multi-Agent Graph
# ---------------------------------------------------------------------------
class SupervisorState(MessagesState):
    next: str

_NEXT_RE = re.compile(r'\{\s*"next"\s*:\s*"([^"]+)"[^}]*\}')

def supervisor_node(state: SupervisorState) -> dict:
    messages = state["messages"]

    # Detect which agents have ALREADY responded (prevent re-routing)
    already_called = set()
    for msg in messages:
        if hasattr(msg, "content") and msg.content:
            tag_match = _AGENT_TAG_RE.match(msg.content)
            if tag_match:
                already_called.add(tag_match.group(1))

    response = llm.invoke([SystemMessage(content=SUPERVISOR_PROMPT)] + messages)
    raw_content = response.content.strip()
    next_agent = "FINISH"
    match = _NEXT_RE.search(raw_content)
    if match:
        candidate = match.group(1)
        if candidate in AGENT_NAMES or candidate == "FINISH":
            next_agent = candidate

    # Guard: never re-route to an agent that already responded
    if next_agent in already_called:
        logger.info(f"Supervisor tried to re-route to {next_agent} (already called) → FINISH")
        next_agent = "FINISH"

    if next_agent not in AGENT_NAMES and next_agent != "FINISH":
        next_agent = "FINISH"

    # FIX: When routing to FINISH, suppress LLM's summary text.
    # The supervisor's job is routing only — sub-agent responses carry the real content.
    # Without this, the LLM's FINISH message (e.g. "The beta is 1.7...") becomes the
    # LAST message in the response, burying the decision_agent's actual BUY/HOLD/SELL signal.
    if next_agent == "FINISH":
        return {"messages": [AIMessage(content='{"next": "FINISH"}')], "next": next_agent}

    return {"messages": [response], "next": next_agent}


# ---------------------------------------------------------------------------
# Post-processing: extract decision signals from tool responses
# ---------------------------------------------------------------------------
_SIGNAL_RE = re.compile(r'"signal"\s*:\s*"(BUY|HOLD|SELL)"')
_RECOMMEND_RE = re.compile(r'(\w+):\s*(BUY|HOLD|SELL)\s+signal\s*\(score\s*([\d.-]+)')
_SIGNAL_WORD_RE = re.compile(r'\b(BUY|HOLD|SELL)\b')

def _extract_decision_signals(new_messages: list) -> str:
    """Extract BUY/HOLD/SELL signals from tool response messages.
    
    UCFunctionToolkit returns tool messages as JSON: {"format": "CSV", "value": "...csv..."}
    This function parses the CSV to extract (symbol, signal, score) tuples.
    """
    import json, csv, io
    
    signals = []  # list of (symbol, signal, score)
    
    for msg in new_messages:
        msg_content = getattr(msg, "content", "") or ""
        if not msg_content or len(msg_content) < 20:
            continue
        
        # Skip pure AI summary messages (no tool data)
        msg_type = getattr(msg, "type", "")
        if msg_type == "ai" and not getattr(msg, "tool_calls", None):
            continue
        
        # Strategy 1: Parse UCFunctionToolkit JSON+CSV format
        # Format: {"format": "CSV", "value": "col1,col2...\nval1,val2...\n"}
        try:
            wrapper = json.loads(msg_content)
            if isinstance(wrapper, dict) and wrapper.get("format") == "CSV":
                csv_text = wrapper.get("value", "")
                reader = csv.DictReader(io.StringIO(csv_text))
                for row in reader:
                    sig = (row.get("signal") or "").strip().upper()
                    if sig in ("BUY", "HOLD", "SELL"):
                        sym = (row.get("symbol") or "?").strip()
                        sc = (row.get("score") or "").strip()
                        signals.append((sym, sig, sc))
        except (json.JSONDecodeError, TypeError, KeyError):
            pass
        
        # Strategy 2: Plain text search for signal words
        if not signals:
            for sig_word in ("BUY", "HOLD", "SELL"):
                if sig_word in msg_content:
                    # Extract any nearby uppercase ticker-like words
                    idx = msg_content.index(sig_word)
                    nearby = msg_content[max(0, idx-80):idx+80]
                    # Simple: look for known common tickers
                    for token in nearby.replace(",", " ").replace(":", " ").split():
                        t = token.strip().upper()
                        if 1 <= len(t) <= 5 and t.isalpha() and t not in ("BUY", "HOLD", "SELL", "THE", "FOR", "AND", "WITH", "CSV", "SIGNAL"):
                            signals.append((t, sig_word, ""))
                            break
                    else:
                        signals.append(("", sig_word, ""))
    
    if not signals:
        return ""
    
    # Deduplicate by (symbol, signal)
    seen = set()
    unique = []
    for sym, sig, sc in signals:
        key = (sym, sig)
        if key not in seen:
            seen.add(key)
            unique.append((sym, sig, sc))
    
    # Format as prefix text
    if len(unique) == 1:
        sym, sig, sc = unique[0]
        score_part = f" with a score of {sc}" if sc else ""
        return f"The signal for {sym} is **{sig}**{score_part}."
    elif len(unique) <= 10:
        parts = []
        for sym, sig, sc in unique[:10]:
            score_part = f" ({sc})" if sc else ""
            parts.append(f"{sym}: **{sig}**{score_part}")
        return "Decision signals: " + ", ".join(parts) + "."
    else:
        buy = [u for u in unique if u[1] == "BUY"]
        top = sorted(buy, key=lambda x: float(x[2]) if x[2] else 0, reverse=True)[:5]
        return "Top **BUY** signals: " + ", ".join(f"{s[0]} ({s[2]})" for s in top) + "."

def agent_node(state: SupervisorState, agent_name: str) -> dict:
    agent = AGENTS[agent_name]
    n_input = len(state["messages"])
    result = agent.invoke({"messages": state["messages"]})
    agent_messages = result.get("messages", [])
    # CRITICAL: Only take NEW messages generated by the sub-agent,
    # not the input history that gets echoed back.
    new_messages = agent_messages[n_input:]

    # --- Post-process: extract signals from tool responses ---
    signal_prefix = ""
    if agent_name == "decision_agent":
        try:
            signal_prefix = _extract_decision_signals(new_messages)
        except Exception:
            pass  # never break the agent on post-processing

    output_messages = []
    ai_messages_collected = []
    for msg in new_messages:
        if hasattr(msg, "content") and msg.content and hasattr(msg, "type") and msg.type == "ai":
            ai_messages_collected.append(msg)
            output_messages.append(AIMessage(content=f"[{agent_name}]: {msg.content}"))

    # Prepend extracted signal to the LAST AI message (the summary)
    if signal_prefix and output_messages:
        last_content = output_messages[-1].content
        # Only prepend if the signal keywords aren't already present
        if not any(kw in last_content for kw in ["BUY", "HOLD", "SELL"]):
            output_messages[-1] = AIMessage(content=f"[{agent_name}]: {signal_prefix}\n\n{ai_messages_collected[-1].content if ai_messages_collected else ''}")

    if not output_messages and new_messages:
        last = new_messages[-1]
        content = f"[{agent_name}]: {last.content if hasattr(last, 'content') else str(last)}"
        if signal_prefix:
            content = f"[{agent_name}]: {signal_prefix}\n\n{last.content if hasattr(last, 'content') else str(last)}"
        output_messages = [AIMessage(content=content)]
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
    result = {"role": role, "content": d.get("content", "") or "", "id": d.get("id") or str(uuid.uuid4())}
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
                messages=[ChatAgentMessage(role="assistant", content=reason, id=str(uuid.uuid4()))]
            )

        self._audit_log(request_id, "REQUEST", user_message=last_msg[:200])

        try:
            request = {"messages": self._convert_messages_to_dict(messages)}
            raw_messages = []
            for event in self.graph.stream(request, stream_mode="updates"):
                for node_data in event.values():
                    raw_messages.extend(
                        ChatAgentMessage(**_lc_msg_to_dict(msg))
                        for msg in node_data.get("messages", [])
                    )

            # --- Filter: keep only meaningful sub-agent responses ---
            output_messages = _extract_final_response(raw_messages)

            # --- Output guardrail ---
            for msg in output_messages:
                if msg.content:
                    msg.content = sanitize_output(msg.content)

            # If filtering removed everything, return last raw message
            if not output_messages and raw_messages:
                last_raw = raw_messages[-1]
                if last_raw.content:
                    last_raw.content = _dedup_text(sanitize_output(last_raw.content))
                output_messages = [last_raw]

            latency_ms = int((time.time() - t0) * 1000)
            self._audit_log(
                request_id, "RESPONSE",
                latency_ms=latency_ms,
                num_messages=len(output_messages),
                raw_messages=len(raw_messages),
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
                    id=str(uuid.uuid4()),
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
                delta=ChatAgentMessage(role="assistant", content=reason, id=str(uuid.uuid4()))
            )
            return

        self._audit_log(request_id, "REQUEST_STREAM", user_message=last_msg[:200])

        try:
            request = {"messages": self._convert_messages_to_dict(messages)}
            chunk_count = 0
            seen_fingerprints = set()
            for event in self.graph.stream(request, stream_mode="updates"):
                for node_data in event.values():
                    for msg in node_data.get("messages", []):
                        agent_msg = ChatAgentMessage(**_lc_msg_to_dict(msg))
                        if not agent_msg.content:
                            continue
                        # Skip routing JSON
                        if _ROUTING_JSON_RE.match(agent_msg.content.strip()):
                            continue
                        # Skip very short noise
                        if len(agent_msg.content.strip()) < 30:
                            continue
                        # Dedup by fingerprint
                        fp = agent_msg.content.strip()[:200]
                        if fp in seen_fingerprints:
                            continue
                        seen_fingerprints.add(fp)
                        # Apply dedup + sanitize
                        agent_msg.content = sanitize_output(_dedup_text(agent_msg.content))
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
                    id=str(uuid.uuid4()),
                )
            )

# ---------------------------------------------------------------------------
# Register with MLflow
# ---------------------------------------------------------------------------
mlflow.models.set_model(RiskBricksSupervisor(multi_agent_graph))
