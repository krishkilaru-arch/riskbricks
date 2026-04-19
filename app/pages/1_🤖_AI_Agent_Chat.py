"""
RiskBricks AI Agent Chat — Streamlit page

Calls the deployed RiskBricks agent serving endpoint for real LLM-powered
financial risk analysis. Falls back to direct UC function queries if the
endpoint is not yet deployed or has permission issues.
"""

import streamlit as st
import os
import json
import re
import requests
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Configurable catalog
CATALOG = os.getenv("RISKBRICKS_CATALOG", "riskbricks")


def _escape_dollars(text: str) -> str:
    """Escape $ signs so Streamlit doesn't render them as LaTeX math."""
    return text.replace("$", "\\$")


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="RiskBricks AI Agent", page_icon="\U0001f916", layout="wide")

st.title("\U0001f916 RiskBricks AI Agent")
st.caption("Ask questions about portfolio risk, holdings, stress tests, forecasts, and macro context.")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AGENT_ENDPOINT = os.getenv("RISKBRICKS_AGENT_ENDPOINT", "riskbricks-supervisor-agent")

@st.cache_resource
def _get_workspace_client():
    return WorkspaceClient()

w = _get_workspace_client()

# ---------------------------------------------------------------------------
# Response cleaning helpers
# ---------------------------------------------------------------------------
_AGENT_PREFIX_RE = re.compile(r"^\[\w+\]:\s*")
_ROUTING_JSON_RE = re.compile(r'\s*\{"next"\s*:\s*"[^"]*"[^}]*\}\s*')


def _clean_message(text: str) -> str:
    """Strip agent prefixes and routing JSON from a message."""
    while _AGENT_PREFIX_RE.match(text):
        text = _AGENT_PREFIX_RE.sub("", text, count=1).strip()
    text = _ROUTING_JSON_RE.sub(" ", text).strip()
    text = re.sub(r"\[\w+\]:\s*", " ", text).strip()
    text = re.sub(r"  +", " ", text)
    return text


def get_agent_response(user_message: str, chat_history: list) -> str:
    """Call the deployed agent serving endpoint."""
    msgs = []
    for msg in chat_history:
        msgs.append({"role": msg["role"], "content": msg["content"]})
    msgs.append({"role": "user", "content": user_message})

    try:
        host = w.config.host.rstrip("/")
        url = f"{host}/serving-endpoints/{AGENT_ENDPOINT}/invocations"
        headers = {"Content-Type": "application/json"}
        headers.update(w.config.authenticate())

        resp = requests.post(url, headers=headers, json={"messages": msgs}, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        # ChatAgent format
        if "messages" in data:
            assistant_msgs = [m for m in data["messages"]
                              if m.get("role") == "assistant" and m.get("content")]
            if assistant_msgs:
                cleaned = []
                for m in assistant_msgs:
                    text = _clean_message(m["content"])
                    if text and len(text) > 20:
                        cleaned.append(text)

                if cleaned:
                    # Prefer the LAST sub-agent tagged message from the
                    # current turn. Using [-1] instead of max(key=len)
                    # prevents stale history from being picked in multi-turn.
                    import re as _re
                    _agent_tag = _re.compile(r'^\[\w+_agent\]:\s*')
                    tagged = [t for t in cleaned if _agent_tag.match(t)]
                    if tagged:
                        best = tagged[-1]
                        return _agent_tag.sub('', best, count=1).strip()
                    # Fallback: last substantive message
                    return cleaned[-1]
                for m in reversed(assistant_msgs):
                    text = _clean_message(m["content"])
                    if text and len(text) > 5:
                        return text
                return assistant_msgs[-1]["content"]

        # OpenAI ChatCompletion format fallback
        if "choices" in data and data["choices"]:
            return data["choices"][0]["message"]["content"]

        if "output" in data:
            return data["output"]

        return json.dumps(data, indent=2)

    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        body = e.response.text[:300] if e.response is not None else str(e)
        if code in (403, 404):
            return _fallback_response(user_message)
        return f"\u26a0\ufe0f Agent endpoint error ({code}): {body}"
    except requests.exceptions.ConnectionError:
        return _fallback_response(user_message)
    except Exception as e:
        return f"\u26a0\ufe0f Error calling agent: {str(e)}"


# ---------------------------------------------------------------------------
# Safe parameter extraction (Issue 2: prevent SQL injection in fallback)
# ---------------------------------------------------------------------------
_KNOWN_MANAGERS = {"sarah russel", "rena tang", "mohit arora"}
_KNOWN_SYMBOLS = {
    "LMT", "RTX", "NOC", "GD", "BA", "HII",
    "XOM", "CVX", "COP", "SLB", "HAL", "OXY",
    "JPM", "BAC", "GS", "MS", "C", "WFC",
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "INTC", "AMD", "AVGO", "QCOM", "MU", "LRCX", "AMAT",
    "WMT", "COST", "HD", "NKE", "MCD", "SBUX",
    "JNJ", "PFE", "UNH", "LLY", "ABBV", "MRK",
    "CAT", "DE", "HON", "GE", "MMM",
    "UAL", "DAL", "AAL",
}


def _extract_manager(query: str) -> str:
    """Extract manager name from query — returns ONLY known safe values."""
    q = query.lower()
    if "sarah" in q:
        return "Sarah Russel"
    if "rena" in q:
        return "Rena Tang"
    if "mohit" in q:
        return "Mohit Arora"
    return "all"


def _extract_symbol(query: str) -> str:
    """Extract stock symbol from query — returns ONLY known safe values."""
    tokens = re.findall(r"\b[A-Z]{1,5}\b", query)
    skip = {"AI", "I", "A", "THE", "FOR", "AND", "OR", "ALL", "IT", "MY", "IS", "AT", "TO", "IN", "ON"}
    for t in tokens:
        if t in _KNOWN_SYMBOLS:
            return t
    for sym in ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META"]:
        if sym.lower() in query.lower():
            return sym
    return "NVDA"


def _safe_uc_call(cursor, func_name: str, *args: str):
    """Safely call a UC function with validated string arguments."""
    # All args are already validated against known-safe values above
    arg_str = ", ".join(f"'{a}'" for a in args)
    cursor.execute(f"SELECT * FROM {CATALOG}.agent_tools.{func_name}({arg_str})")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in rows]


def _fallback_response(query: str) -> str:
    """Fallback: query UC functions directly via SQL when endpoint is unavailable."""
    try:
        from databricks import sql as dbsql
        from databricks.sdk.core import Config

        cfg = Config()
        host = (cfg.host or "").replace("https://", "")
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

        if not warehouse_id:
            return (
                "\u26a0\ufe0f Agent endpoint is not deployed yet. "
                "Run `notebooks/agents/03_deploy_agent` to deploy, "
                "or set DATABRICKS_WAREHOUSE_ID for direct SQL fallback."
            )

        conn = dbsql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            credentials_provider=lambda: cfg.authenticate,
        )
        cursor = conn.cursor()

        q = query.lower()
        results = []
        title = "Query Results"

        # Route to appropriate UC function based on intent — all inputs are safe
        if any(kw in q for kw in ["risk", "var", "volatility", "beta", "compare", "metric"]):
            manager = _extract_manager(q)
            results = _safe_uc_call(cursor, "get_portfolio_risk_metrics", manager)
            title = f"Portfolio Risk Metrics ({manager})"

        elif any(kw in q for kw in ["stress", "crash", "drawdown", "recession"]):
            manager = _extract_manager(q)
            results = _safe_uc_call(cursor, "get_stress_test_results", manager)
            title = f"Stress Tests ({manager})"

        elif any(kw in q for kw in ["holding", "position", "stock"]):
            manager = _extract_manager(q)
            results = _safe_uc_call(cursor, "get_portfolio_holdings", manager)
            title = f"Holdings ({manager})"

        elif any(kw in q for kw in ["sector", "allocation", "exposure"]):
            manager = _extract_manager(q)
            results = _safe_uc_call(cursor, "get_sector_exposures", manager)
            title = f"Sector Exposures ({manager})"

        elif any(kw in q for kw in ["macro", "fed", "rate", "gdp", "cpi", "vix"]):
            cursor.execute(f"SELECT * FROM {CATALOG}.agent_tools.get_macro_context()")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = "Macro Context"

        elif any(kw in q for kw in ["forecast", "predict", "price target", "ml prediction"]):
            symbol = _extract_symbol(q)
            results = _safe_uc_call(cursor, "get_stock_forecast", symbol)
            title = f"Forecast ({symbol})"

        elif any(kw in q for kw in ["signal", "buy", "sell", "hold", "decision"]):
            symbol = _extract_symbol(q)
            results = _safe_uc_call(cursor, "get_decision_signal", symbol)
            title = f"Decision Signal ({symbol})"

        else:
            results = _safe_uc_call(cursor, "get_portfolio_risk_metrics", "all")
            title = "Portfolio Overview"

        cursor.close()
        conn.close()

        if not results:
            return "No data found for that query."

        # Format as markdown table
        md = f"**{title}** _(direct SQL \u2014 agent endpoint not available)_\n\n"
        cols = list(results[0].keys())
        md += "| " + " | ".join(cols) + " |\n"
        md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
        for row in results:
            md += "| " + " | ".join(str(row.get(c, "")) for c in cols) + " |\n"
        return md

    except Exception as e:
        return f"\u26a0\ufe0f Fallback query failed: {str(e)}"


# ---------------------------------------------------------------------------
# Chat UI
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("\U0001f4a1 Example Queries")
    examples = [
        "Compare risk metrics for all three managers",
        "Show me Mohit Arora's top holdings and sector exposure",
        "What stress test scenario hurts Sarah Russel the most?",
        "Give me the forecast for NVDA",
        "What's the current macro environment?",
        "Which stocks have a Buy signal?",
        "Show factor exposures for AAPL",
        "Complete risk report for Rena Tang",
    ]
    for ex in examples:
        if st.button(ex, key=f"ex_{hash(ex)}", use_container_width=True):
            st.session_state.pending_query = ex

    st.divider()
    st.subheader("\u2699\ufe0f Status")
    st.text(f"Endpoint: {AGENT_ENDPOINT}")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(_escape_dollars(message["content"]) if message["role"] == "assistant" else message["content"])

if "pending_query" in st.session_state:
    user_input = st.session_state.pending_query
    del st.session_state.pending_query

    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            response = get_agent_response(user_input, st.session_state.messages[:-1])
        st.markdown(_escape_dollars(response))
    st.session_state.messages.append({"role": "assistant", "content": response})
    st.rerun()

if user_input := st.chat_input("Ask about portfolio risk, holdings, forecasts..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            response = get_agent_response(user_input, st.session_state.messages[:-1])
        st.markdown(_escape_dollars(response))
    st.session_state.messages.append({"role": "assistant", "content": response})
