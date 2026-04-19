#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# NOTE: Mosaic AI Agent Framework API/CLI commands can vary by workspace version.
# If your workspace supports the Agents API, this will work as-is:
#   databricks api post /api/2.0/agents --json @<agent_config.json>
# If your workspace exposes agents via serving endpoints instead, replace the
# create_agent function with a serving-endpoints create call and use the
# appropriate endpoint JSON schema.

create_agent() {
  local name="$1"
  local cfg="$2"
  echo "Creating agent: ${name}"
  databricks api post /api/2.0/agents --json @"${cfg}"
}

create_agent "riskbricks-retrieval-agent" "${ROOT}/retrieval_agent.json"
create_agent "riskbricks-forecast-agent" "${ROOT}/forecast_agent.json"
create_agent "riskbricks-risk-agent" "${ROOT}/risk_agent.json"
create_agent "riskbricks-factor-agent" "${ROOT}/factor_agent.json"
create_agent "riskbricks-decision-agent" "${ROOT}/decision_agent.json"
create_agent "riskbricks-evaluation-agent" "${ROOT}/evaluation_agent.json"
create_agent "riskbricks-portfolio-agent" "${ROOT}/portfolio_agent.json"
create_agent "riskbricks-supervisor-agent" "${ROOT}/supervisor_agent.json"

echo "✅ Agent creation requests submitted."
