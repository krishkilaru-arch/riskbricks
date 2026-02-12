#!/bin/bash
# Deploy RiskBricks AI Agents - Official Databricks Pattern
# This script deploys all agents using the ResponsesAgent pattern

set -e

echo "🤖 Deploying RiskBricks AI Agents (Official Pattern)"
echo "=================================================="
echo ""

# Configuration
WORKSPACE_PATH="/Shared/riskbricks/notebooks/mosaic_agents"
LOCAL_PATH="notebooks/mosaic_agents"

# Check Databricks CLI
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: Databricks CLI not found"
    echo "   Install: pip install databricks-cli"
    exit 1
fi

echo "✅ Databricks CLI found"
echo ""

# Upload agent base
echo "📦 Uploading agent_base.py..."
databricks workspace import \
  --file "${LOCAL_PATH}/agent_base.py" \
  "${WORKSPACE_PATH}/agent_base.py" \
  --language PYTHON \
  --overwrite

echo "✅ agent_base.py uploaded"
echo ""

# Upload deployment notebook
echo "📦 Uploading deployment notebook..."
databricks workspace import \
  --file "${LOCAL_PATH}/deploy_agents_official_pattern.py" \
  "${WORKSPACE_PATH}/deploy_agents_official_pattern" \
  --language PYTHON \
  --overwrite

echo "✅ deploy_agents_official_pattern.py uploaded"
echo ""

echo "🎉 Files uploaded successfully!"
echo ""
echo "Next steps:"
echo "1. Navigate to Databricks workspace"
echo "2. Open: ${WORKSPACE_PATH}/deploy_agents_official_pattern"
echo "3. Run all cells (this will take ~10-15 minutes)"
echo "4. Verify endpoints in Machine Learning → Serving"
echo ""
echo "Or run notebook programmatically:"
echo "  databricks jobs runs submit \\"
echo "    --notebook-path ${WORKSPACE_PATH}/deploy_agents_official_pattern"
echo ""
