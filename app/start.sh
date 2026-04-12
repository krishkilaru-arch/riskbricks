#!/bin/bash
# Startup script for Databricks Apps that properly handles PORT environment variable

# Use PORT if provided by Databricks, otherwise default to 8501
PORT=${PORT:-8501}

echo "Starting Streamlit on port $PORT"

streamlit run Home.py \
  --server.port=$PORT \
  --server.address=0.0.0.0 \
  --server.enableCORS=false \
  --browser.gatherUsageStats=false
