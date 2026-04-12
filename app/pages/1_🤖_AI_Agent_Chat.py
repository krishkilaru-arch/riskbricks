"""
RiskBricks AI Agent Chat — Streamlit page

Calls the deployed RiskBricks agent serving endpoint for real LLM-powered
financial risk analysis. Falls back to direct UC function queries if the
endpoint is not yet deployed.
"""

import streamlit as st
import os
import json
import requests
from datetime import datetime
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="RiskBricks AI Agent", page_icon="🤖", layout="wide")

st.title("🤖 RiskBricks AI Agent")
st.caption("Ask questions about portfolio risk, holdings, stress tests, forecasts, and macro context.")

# ---------------------------------------------------------------------------
# Configuration — uses Databricks SDK unified auth (OAuth via App resources)
# ---------------------------------------------------------------------------
AGENT_ENDPOINT = os.getenv("RISKBRICKS_AGENT_ENDPOINT", "agents_riskbricks-agents-riskbricks_agent")

@st.cache_resource
def _get_workspace_client():
    return WorkspaceClient()

w = _get_workspace_client()


def get_agent_response(user_message: str, chat_history: list) -> str:
    """Call the deployed agent serving endpoint.

    Uses raw HTTP with SDK-generated OAuth headers so we can parse
    the ChatAgent response format (messages[], not choices[]).
    """
    msgs = []
    for msg in chat_history:
        msgs.append({"role": msg["role"], "content": msg["content"]})
    msgs.append({"role": "user", "content": user_message})

    try:
        # Build URL and auth headers from SDK config
        host = w.config.host.rstrip("/")
        url = f"{host}/serving-endpoints/{AGENT_ENDPOINT}/invocations"
        headers = {"Content-Type": "application/json"}
        headers.update(w.config.authenticate())

        resp = requests.post(url, headers=headers, json={"messages": msgs}, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        # ChatAgent format: {"messages": [{"role": "assistant", "content": "..."}]}
        if "messages" in data:
            assistant_msgs = [m for m in data["messages"]
                              if m.get("role") == "assistant" and m.get("content")]
            if assistant_msgs:
                return assistant_msgs[-1]["content"]

        # OpenAI ChatCompletion format fallback
        if "choices" in data and data["choices"]:
            return data["choices"][0]["message"]["content"]

        # Generic fallback
        if "output" in data:
            return data["output"]

        return json.dumps(data, indent=2)

    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        body = e.response.text[:300] if e.response is not None else str(e)
        if code == 404:
            return _fallback_response(user_message)
        return f"⚠️ Agent endpoint error ({code}): {body}"
    except Exception as e:
        return f"⚠️ Error calling agent: {str(e)}"


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
                "⚠️ Agent endpoint is not deployed yet. "
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

        # Route to appropriate UC function based on intent
        if any(kw in q for kw in ["risk", "var", "volatility", "beta"]):
            manager = _extract_manager(q)
            cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_portfolio_risk_metrics(\'{manager}\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = f"Portfolio Risk Metrics ({manager})"

        elif any(kw in q for kw in ["stress", "crash", "drawdown", "recession"]):
            manager = _extract_manager(q)
            cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_stress_test_results(\'{manager}\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = f"Stress Tests ({manager})"

        elif any(kw in q for kw in ["holding", "position", "stock"]):
            manager = _extract_manager(q)
            cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_portfolio_holdings(\'{manager}\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = f"Holdings ({manager})"

        elif any(kw in q for kw in ["sector", "allocation", "exposure"]):
            manager = _extract_manager(q)
            cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_sector_exposures(\'{manager}\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = f"Sector Exposures ({manager})"

        elif any(kw in q for kw in ["macro", "fed", "rate", "gdp", "cpi", "vix"]):
            cursor.execute("SELECT * FROM riskbricks.agent_tools.get_macro_context()")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = "Macro Context"

        elif any(kw in q for kw in ["forecast", "predict", "price target"]):
            symbol = _extract_symbol(q)
            cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_stock_forecast(\'{symbol}\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = f"Forecast ({symbol})"

        elif any(kw in q for kw in ["signal", "buy", "sell", "hold", "decision"]):
            symbol = _extract_symbol(q)
            cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_decision_signal(\'{symbol}\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = f"Decision Signal ({symbol})"

        else:
            cursor.execute("SELECT * FROM riskbricks.agent_tools.get_portfolio_risk_metrics(\'all\')")
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            results = [dict(zip(cols, r)) for r in rows]
            title = "Portfolio Overview"

        cursor.close()
        conn.close()

        if not results:
            return "No data found for that query."

        # Format as markdown table
        md = f"**{title}** _(direct SQL — agent endpoint not deployed)_\n\n"
        cols = list(results[0].keys())
        md += "| " + " | ".join(cols) + " |\n"
        md += "| " + " | ".join(["---"] * len(cols)) + " |\n"
        for row in results:
            md += "| " + " | ".join(str(row.get(c, "")) for c in cols) + " |\n"
        return md

    except Exception as e:
        return f"⚠️ Fallback query failed: {str(e)}"


def _extract_manager(query: str) -> str:
    q = query.lower()
    if "sarah" in q:
        return "Sarah Russel"
    if "rena" in q:
        return "Rena Tang"
    if "mohit" in q:
        return "Mohit Arora"
    return "all"


def _extract_symbol(query: str) -> str:
    import re
    tokens = re.findall(r"\b[A-Z]{1,5}\b", query)
    skip = {"AI", "I", "A", "THE", "FOR", "AND", "OR", "ALL", "IT", "MY", "IS", "AT", "TO", "IN", "ON"}
    for t in tokens:
        if t not in skip:
            return t
    for sym in ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META"]:
        if sym.lower() in query.lower():
            return sym
    return "NVDA"


# ---------------------------------------------------------------------------
# Chat UI
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("💡 Example Queries")
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
    st.subheader("⚙️ Status")
    st.text(f"Endpoint: {AGENT_ENDPOINT}")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if "pending_query" in st.session_state:
    user_input = st.session_state.pending_query
    del st.session_state.pending_query

    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            response = get_agent_response(user_input, st.session_state.messages[:-1])
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})
    st.rerun()

if user_input := st.chat_input("Ask about portfolio risk, holdings, forecasts..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            response = get_agent_response(user_input, st.session_state.messages[:-1])
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})
