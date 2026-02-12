#!/usr/bin/env bash
set -euo pipefail

# Deploy all Databricks Workflow jobs from YAML files

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="${ROOT}/jobs"

echo "======================================"
echo "🚀 Deploying RiskBricks Workflows"
echo "======================================"
echo ""

# Check if databricks CLI is available
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: databricks CLI not found"
    echo "Install: pip install databricks-cli"
    exit 1
fi

# Check if databricks bundle is configured
if ! databricks bundle validate --root "${ROOT}" &> /dev/null; then
    echo "⚠️  Warning: Databricks bundle validation failed"
    echo "    Continuing with individual job deployments..."
fi

echo "📂 Jobs directory: ${JOBS_DIR}"
echo ""

# Count YAML files
yaml_count=$(find "${JOBS_DIR}" -name "*.yml" -type f | wc -l | tr -d ' ')
echo "Found ${yaml_count} workflow YAML files"
echo ""

# Deploy each job
deployed=0
failed=0

for yaml_file in "${JOBS_DIR}"/*.yml; do
    if [ ! -f "${yaml_file}" ]; then
        continue
    fi
    
    job_name=$(basename "${yaml_file}" .yml)
    echo "---"
    echo "📝 Deploying: ${job_name}"
    
    # Try using databricks bundle deploy (recommended)
    if databricks bundle deploy --root "${ROOT}" &> /dev/null; then
        echo "   ✅ Deployed via bundle"
        ((deployed++))
    else
        # Fallback: Use Jobs API directly
        echo "   ⚠️  Bundle deploy failed, trying Jobs API..."
        
        # Note: This requires manual job creation or using the Jobs API
        # The YAML format for `databricks bundle` is different from Jobs API JSON
        echo "   ℹ️  Please deploy manually via Databricks UI or use bundle:"
        echo "      databricks jobs create --json-file ${yaml_file}"
        echo "   OR"
        echo "      Use Databricks UI: Workflows → Create Job → Import YAML"
        ((failed++))
    fi
done

echo ""
echo "======================================"
echo "📊 Deployment Summary"
echo "======================================"
echo "Total jobs: ${yaml_count}"
echo "Deployed: ${deployed}"
echo "Failed: ${failed}"
echo ""

if [ $failed -eq 0 ]; then
    echo "✅ All workflows deployed successfully!"
else
    echo "⚠️  Some workflows require manual deployment"
    echo ""
    echo "Manual Deployment Steps:"
    echo "1. Go to Databricks Workspace → Workflows → Jobs"
    echo "2. Click 'Create Job'"
    echo "3. Import each YAML file from: ${JOBS_DIR}"
    echo ""
    echo "OR use Databricks CLI:"
    echo "   databricks bundle deploy --root ${ROOT}"
fi

echo ""
echo "🔍 To verify deployed jobs:"
echo "   databricks jobs list | grep riskbricks"
echo ""
echo "📖 Documentation:"
echo "   https://docs.databricks.com/workflows/jobs/index.html"
echo ""

# List available jobs
echo "📋 Available workflow files:"
for yaml_file in "${JOBS_DIR}"/*.yml; do
    if [ -f "${yaml_file}" ]; then
        job_name=$(basename "${yaml_file}" .yml)
        echo "   - ${job_name}"
    fi
done

echo ""
echo "======================================"
