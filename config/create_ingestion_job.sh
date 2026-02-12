#!/bin/bash

# RiskBricks - Create Data Ingestion Job
# This script creates a Databricks Job that runs every 15 minutes

set -e

echo "🚀 Creating RiskBricks Data Ingestion Job..."
echo ""

# Set config profile
export DATABRICKS_CONFIG_PROFILE=dev

# Get workspace URL from config
WORKSPACE_URL=$(grep 'host' .databrickscfg | head -1 | cut -d'=' -f2 | tr -d ' ')

echo "📍 Workspace: $WORKSPACE_URL"
echo ""

# Create job configuration with new cluster
cat > /tmp/riskbricks_job.json <<'EOF'
{
  "name": "RiskBricks - Data Ingestion (Every 15 min)",
  "email_notifications": {
    "no_alert_for_skipped_runs": false
  },
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "data_ingestion",
      "notebook_task": {
        "notebook_path": "/Shared/RiskBricks/files/notebooks/ingestion/01_data_ingestion",
        "source": "WORKSPACE"
      },
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.preview.enabled": "true"
        }
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 */15 * * * ?",
    "timezone_id": "America/New_York",
    "pause_status": "UNPAUSED"
  },
  "format": "MULTI_TASK"
}
EOF

echo "📋 Job Configuration:"
cat /tmp/riskbricks_job.json | jq '.' 2>/dev/null || cat /tmp/riskbricks_job.json
echo ""

# Create the job
echo "🔨 Creating job in Databricks..."
JOB_ID=$(databricks jobs create --json-file /tmp/riskbricks_job.json | jq -r '.job_id')

if [ ! -z "$JOB_ID" ]; then
    echo ""
    echo "✅ SUCCESS! Job created with ID: $JOB_ID"
    echo ""
    echo "📊 Job Details:"
    echo "   Name: RiskBricks - Data Ingestion (Every 15 min)"
    echo "   Schedule: Every 15 minutes"
    echo "   Timezone: America/New_York (ET)"
    echo "   Status: ACTIVE"
    echo ""
    echo "🌐 View Job:"
    echo "   $WORKSPACE_URL/jobs/$JOB_ID"
    echo ""
    echo "🎮 Run Job Now:"
    echo "   databricks jobs run-now --job-id $JOB_ID"
    echo ""
    echo "⏸️  Pause Job:"
    echo "   databricks jobs update --job-id $JOB_ID --json '{\"schedule\": {\"pause_status\": \"PAUSED\"}}'"
    echo ""
else
    echo "❌ Failed to create job. Check your Databricks CLI configuration."
    exit 1
fi

# Clean up
rm /tmp/riskbricks_job.json

echo "✅ Done!"
