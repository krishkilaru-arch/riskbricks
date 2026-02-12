#!/bin/bash
# Deploy Streamlit app to Databricks Apps

set -e

echo "🚀 Deploying RiskBricks Streamlit App..."

cd /Users/analytics360/databricks/new_dais_2026/riskbricks

echo "📦 Deploying app to Databricks Apps..."
databricks apps deploy riskbricks-app --source-code-path app/ -v

echo "✅ Deployment complete!"
echo "🌐 URL: https://riskbricks-app-7474660441663212.aws.databricksapps.com"
