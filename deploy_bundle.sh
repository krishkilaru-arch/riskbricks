#!/bin/bash
# Deploy RiskBricks AI Agents using Databricks Asset Bundles

set -e

echo "🚀 RiskBricks AI Agents - Bundle Deployment"
echo "============================================"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: Databricks CLI not found"
    echo "   Install: pip install databricks-cli"
    exit 1
fi

echo "✅ Databricks CLI found"
echo ""

# Get target environment (default: dev)
TARGET="${1:-dev}"
echo "📍 Target environment: $TARGET"
echo ""

# Validate bundle configuration
echo "🔍 Validating bundle configuration..."
if databricks bundle validate -t "$TARGET"; then
    echo "✅ Bundle configuration is valid"
else
    echo "❌ Bundle validation failed"
    exit 1
fi
echo ""

# Deploy the bundle
echo "📦 Deploying bundle to $TARGET..."
if databricks bundle deploy -t "$TARGET"; then
    echo "✅ Bundle deployed successfully"
else
    echo "❌ Bundle deployment failed"
    exit 1
fi
echo ""

# Ask if user wants to run the deployment job
read -p "🤔 Do you want to run the agent deployment job now? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🎬 Running deployment job..."
    if databricks bundle run deploy_agents -t "$TARGET"; then
        echo "✅ Deployment job started successfully"
    else
        echo "❌ Failed to start deployment job"
        exit 1
    fi
    echo ""
    echo "📊 Monitor the job in Databricks UI:"
    echo "   Workflows → Jobs → 'Deploy RiskBricks AI Agents'"
fi

echo ""
echo "🎉 Deployment Complete!"
echo ""
echo "Next steps:"
echo "1. Go to Databricks UI → Workflows → Jobs"
echo "2. Find job: 'Deploy RiskBricks AI Agents'"
echo "3. Monitor deployment progress"
echo "4. After completion, verify in: Machine Learning → Serving"
echo ""
echo "Expected endpoints:"
echo "  - ${TARGET}_forecast_agent"
echo "  - ${TARGET}_risk_agent"
echo "  - ${TARGET}_decision_agent"
echo "  - ${TARGET}_supervisor_agent"
echo ""
