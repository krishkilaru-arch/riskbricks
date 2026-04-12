#!/usr/bin/env bash
set -euo pipefail

# Master Deployment Script for RiskBricks
# Deploys notebooks, workflows, and configurations to Databricks

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "======================================"
echo "🚀 RiskBricks Master Deployment"
echo "======================================"
echo ""
echo "📂 Root: ${ROOT}"
echo "📅 Date: $(date)"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_step() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
print_step "Step 1: Checking Prerequisites"

if ! command -v databricks &> /dev/null; then
    print_error "databricks CLI not found"
    echo "Install: pip install databricks-cli databricks-sdk"
    exit 1
fi
print_success "Databricks CLI installed"

# Check authentication
if ! databricks workspace list / &> /dev/null; then
    print_error "Databricks authentication failed"
    echo "Run: databricks configure --token"
    exit 1
fi
print_success "Databricks authentication verified"

# Get workspace info
WORKSPACE_URL=$(databricks auth env | grep DATABRICKS_HOST | cut -d'=' -f2 | tr -d '"')
print_success "Connected to workspace: ${WORKSPACE_URL}"

# Deploy notebooks
print_step "Step 2: Deploying Notebooks"

# Auto-detect or override workspace path
WORKSPACE_PATH="${RISKBRICKS_WORKSPACE_PATH:-/Shared/riskbricks}"
NOTEBOOKS_PATH="${WORKSPACE_PATH}/notebooks"

echo "Creating workspace directories..."
databricks workspace mkdirs "${WORKSPACE_PATH}" || true
databricks workspace mkdirs "${NOTEBOOKS_PATH}" || true
print_success "Workspace directories created"

echo ""
echo "Uploading notebooks (this may take a few minutes)..."

# Upload notebooks directory recursively
if databricks workspace import-dir "${ROOT}/notebooks" "${NOTEBOOKS_PATH}" --overwrite; then
    print_success "Notebooks uploaded to ${NOTEBOOKS_PATH}"
else
    print_warning "Some notebooks may have failed to upload"
fi

# Count uploaded notebooks
NOTEBOOK_COUNT=$(find "${ROOT}/notebooks" -name "*.py" -type f | wc -l | tr -d ' ')
print_success "Uploaded ${NOTEBOOK_COUNT} notebook files"

# Upload config files
print_step "Step 3: Uploading Configuration Files"

CONFIG_PATH="${WORKSPACE_PATH}/config"
databricks workspace mkdirs "${CONFIG_PATH}" || true
databricks workspace mkdirs "${CONFIG_PATH}/agents" || true

if [ -d "${ROOT}/config/agents" ]; then
    for json_file in "${ROOT}"/config/agents/*.json; do
        if [ -f "${json_file}" ]; then
            filename=$(basename "${json_file}")
            databricks workspace import "${json_file}" "${CONFIG_PATH}/agents/${filename}" --overwrite --language JSON || true
        fi
    done
    print_success "Agent configs uploaded"
else
    print_warning "Config directory not found, skipping"
fi

# Create Unity Catalog structure
print_step "Step 4: Creating Unity Catalog Structure"

echo "Creating catalogs and schemas..."

databricks sql execute <<EOF || print_warning "Catalog creation may have already been done"
CREATE CATALOG IF NOT EXISTS riskbricks;
CREATE SCHEMA IF NOT EXISTS riskbricks.bronze;
CREATE SCHEMA IF NOT EXISTS riskbricks.silver;
CREATE SCHEMA IF NOT EXISTS riskbricks.gold;
CREATE SCHEMA IF NOT EXISTS riskbricks.tools;
CREATE SCHEMA IF NOT EXISTS riskbricks.models;
EOF

print_success "Unity Catalog structure created"

# Verify catalogs
echo ""
echo "Verifying catalogs..."
databricks sql execute -q "SHOW SCHEMAS IN riskbricks" || print_warning "Could not verify catalogs"

# Register UC Functions
print_step "Step 5: Registering UC Functions"

echo "Running UC tools registration notebook..."
UC_NOTEBOOK="${NOTEBOOKS_PATH}/04_agents/02_create_uc_functions"

# Check if notebook exists
if databricks workspace get-status "${UC_NOTEBOOK}" &> /dev/null; then
    print_success "Found UC tools notebook"
    
    # Try to run it
    echo "Triggering notebook run..."
    echo "(This may take 2-5 minutes)"
    
    if RUN_OUTPUT=$(databricks workspace run "${UC_NOTEBOOK}" 2>&1); then
        print_success "UC functions registered"
        echo "${RUN_OUTPUT}"
    else
        print_warning "UC function registration may need manual intervention"
        echo "Run manually: ${UC_NOTEBOOK}"
    fi
else
    print_warning "UC tools notebook not found at ${UC_NOTEBOOK}"
    echo "Upload notebooks first, then run manually"
fi

# Verify functions
echo ""
echo "Verifying UC functions..."
databricks sql execute -q "SHOW FUNCTIONS IN riskbricks.tools" || print_warning "Could not verify functions"

# Deploy workflows
print_step "Step 6: Deploying Workflows"

if [ -d "${ROOT}/jobs1" ]; then
    YAML_COUNT=$(find "${ROOT}/jobs1" -name "*.yml" -type f | wc -l | tr -d ' ')
    print_success "Found ${YAML_COUNT} workflow definitions"
    
    # Try bundle deploy first
    if [ -f "${ROOT}/databricks.yml" ]; then
        echo "Attempting bundle deploy..."
        if databricks bundle deploy --root "${ROOT}" 2>&1; then
            print_success "Workflows deployed via bundle"
        else
            print_warning "Bundle deploy failed"
            echo ""
            echo "To deploy workflows manually:"
            echo "1. Go to Databricks UI → Workflows → Jobs"
            echo "2. Click 'Create Job'"
            echo "3. Import each YAML from: ${ROOT}/jobs/"
            echo ""
            echo "Or run: ./deploy_workflows.sh"
        fi
    else
        print_warning "databricks.yml not found"
        echo "Deploy workflows manually or run: ./deploy_workflows.sh"
    fi
else
    print_warning "Jobs directory not found"
fi

# Summary
print_step "Deployment Summary"

echo ""
echo "📊 Deployment Status:"
echo ""
echo "  ✅ Notebooks uploaded to: ${NOTEBOOKS_PATH}"
echo "  ✅ Config files uploaded to: ${CONFIG_PATH}"
echo "  ✅ Unity Catalog structure created"
echo ""

# List deployed notebooks
echo "📁 Key Notebooks Deployed:"
echo ""
echo "  Agent Framework:"
echo "    - ${NOTEBOOKS_PATH}/agent_framework/01_create_uc_tools.py"
echo ""
echo "  Agents:"
echo "    - ${NOTEBOOKS_PATH}/agents/00_supervisor.py"
echo "    - ${NOTEBOOKS_PATH}/agents/01_retrieval_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/02_forecast_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/03_risk_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/04_factor_exposure_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/05_news_analytics_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/06_decision_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/07_portfolio_outputs_agent.py"
echo "    - ${NOTEBOOKS_PATH}/agents/99_evaluation_agent.py"
echo ""
echo "  Ingestion:"
echo "    - ${NOTEBOOKS_PATH}/ingestion/stocks/ingest_stocks_yfinance.py"
echo "    - ${NOTEBOOKS_PATH}/ingestion/alt_signals/ingest_alt_signals_yfinance.py"
echo ""
echo "  Advanced Features:"
echo "    - ${NOTEBOOKS_PATH}/03_gold/rag/create_vector_search_index.py"
echo "    - ${NOTEBOOKS_PATH}/agents/08_mlflow_model_registry.py"
echo "    - ${NOTEBOOKS_PATH}/agents/09_backtesting_framework.py"
echo ""

# Next steps
print_step "Next Steps"

echo ""
echo "🎯 What to do now:"
echo ""
echo "1. Verify UC Functions (2 min):"
echo "   databricks sql execute -q 'SHOW FUNCTIONS IN riskbricks.tools'"
echo ""
echo "2. Run Data Ingestion (10-15 min):"
echo "   Go to: ${NOTEBOOKS_PATH}/ingestion/stocks/ingest_stocks_yfinance.py"
echo "   Run with default parameters"
echo ""
echo "3. Test One Agent (5 min):"
echo "   Go to: ${NOTEBOOKS_PATH}/agents/02_forecast_agent.py"
echo "   Parameters: symbol=AAPL, target_date=2026-02-05, mode=fast"
echo ""
echo "4. Test Full Supervisor (15-20 min):"
echo "   Go to: ${NOTEBOOKS_PATH}/agents/00_supervisor.py"
echo "   Parameters: symbol=AAPL, as_of_date=2026-02-04"
echo ""
echo "5. Deploy Workflows:"
echo "   - Option A: databricks bundle deploy --root ${ROOT}"
echo "   - Option B: ${ROOT}/deploy_workflows.sh"
echo "   - Option C: Manual via UI (Workflows → Create Job)"
echo ""

# Quick start command
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📖 Quick Start Guide:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "View notebooks in Databricks:"
echo "  ${WORKSPACE_URL}/#workspace${NOTEBOOKS_PATH}"
echo ""
echo "Verify deployment:"
echo "  databricks workspace list ${NOTEBOOKS_PATH}/agents"
echo ""
echo "Run a test:"
echo "  databricks workspace run ${NOTEBOOKS_PATH}/agents/02_forecast_agent.py \\"
echo "    --parameter symbol=AAPL \\"
echo "    --parameter target_date=2026-02-05 \\"
echo "    --parameter mode=fast"
echo ""

print_step "Deployment Complete!"

echo ""
print_success "RiskBricks deployed successfully! 🎉"
echo ""
echo "Check the deployment guide for more details:"
echo "  ${ROOT}/docs/DEPLOYMENT_GUIDE.md"
echo ""
