#!/bin/bash
# Deploy Mosaic AI Agents to Databricks

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_DIR="$ROOT/config/mosaic_agents"

echo "🤖 Deploying Mosaic AI Agents to Databricks"
echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
echo ""

# Check if databricks CLI is configured
if ! databricks workspace list > /dev/null 2>&1; then
    echo "❌ Databricks CLI not configured"
    echo "Run: databricks configure --token"
    exit 1
fi

echo "✅ Databricks CLI configured"
echo ""

# Function to deploy an agent
deploy_agent() {
    local config_file="$1"
    local agent_name=$(basename "$config_file" .json | cut -d'_' -f2-)
    
    echo "📤 Deploying: $agent_name"
    echo "   Config: $config_file"
    
    # Create agent using Databricks API
    response=$(databricks api post /api/2.0/serving-endpoints/agents/create \
        --json @"$config_file" 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "   ✅ Success"
    else
        echo "   ⚠️  Warning: $response"
    fi
    
    echo ""
}

# Deploy UC Functions first
echo "🧰 Step 1: Deploy Unity Catalog Functions"
echo "   Run notebook: /Shared/riskbricks/notebooks/mosaic_agents/create_all_uc_functions"
echo ""
echo "   To run manually:"
echo "   databricks workspace export /Shared/riskbricks/notebooks/mosaic_agents/create_all_uc_functions"
echo ""

# Ask user to confirm UC functions are deployed
read -p "Have you deployed UC functions? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Please deploy UC functions first"
    exit 1
fi

# Deploy agents
echo "🤖 Step 2: Deploy Mosaic AI Agents"
echo ""

if [ -d "$CONFIG_DIR" ]; then
    for config in "$CONFIG_DIR"/*.json; do
        if [ -f "$config" ]; then
            deploy_agent "$config"
        fi
    done
else
    echo "❌ Config directory not found: $CONFIG_DIR"
    exit 1
fi

echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
echo "✅ Mosaic AI Agents Deployment Complete!"
echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
echo ""
echo "📋 Deployed Agents:"
echo "   - riskbricks_forecast_agent"
echo "   - riskbricks_risk_agent"
echo "   - riskbricks_decision_agent"
echo "   - riskbricks_supervisor"
echo ""
echo "🔗 Access agents at:"
echo "   https://<workspace>.databricks.com/ml/agents"
echo ""
echo "💬 Test agents with:"
echo "   databricks api post /api/2.0/serving-endpoints/agents/<agent-id>/chat \\"
echo "     --json '{\"messages\": [{\"role\": \"user\", \"content\": \"What is the forecast for AAPL?\"}]}'"
echo ""
