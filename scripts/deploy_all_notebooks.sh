#!/bin/bash
# Deploy all notebooks to Databricks workspace

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NOTEBOOKS_DIR="$ROOT/notebooks"
WORKSPACE_BASE="/Shared/riskbricks/notebooks"

echo "📦 Deploying all notebooks to Databricks..."
echo "Source: $NOTEBOOKS_DIR"
echo "Target: $WORKSPACE_BASE"
echo ""

count=0
failed=0

# Find all Python notebooks
while IFS= read -r -d '' notebook; do
    # Get relative path
    rel_path="${notebook#$NOTEBOOKS_DIR/}"
    
    # Remove .py extension for workspace path
    base_name="${rel_path%.py}"
    
    # Construct workspace path
    workspace_path="$WORKSPACE_BASE/$base_name"
    
    echo "📤 Deploying: $rel_path"
    
    # Delete old version (suppress errors if doesn't exist)
    databricks workspace delete "$workspace_path" 2>/dev/null || true
    
    # Import new version
    if databricks workspace import --file "$notebook" "$workspace_path" --language PYTHON; then
        count=$((count + 1))
    else
        echo "❌ Failed: $rel_path"
        failed=$((failed + 1))
    fi
    
done < <(find "$NOTEBOOKS_DIR" -name "*.py" -type f -print0 | sort -z)

echo ""
echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
echo "✅ Successfully deployed: $count notebooks"
if [ $failed -gt 0 ]; then
    echo "❌ Failed: $failed notebooks"
fi
echo "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "=" "="
