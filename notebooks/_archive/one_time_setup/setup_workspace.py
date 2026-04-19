# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 RiskBricks Workspace Setup
# MAGIC
# MAGIC Run this notebook **once** in a new workspace to initialize all prerequisites.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Creates the `riskbricks` Unity Catalog and schemas
# MAGIC 2. Validates secrets scope exists
# MAGIC 3. Checks model endpoint availability
# MAGIC 4. Validates config auto-detection
# MAGIC 5. Prints next steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog & Schemas

# COMMAND ----------

catalog = "riskbricks"
schemas = ["bronze", "silver", "gold", "agent_tools", "functions", "tools", "models", "agents"]

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
print(f"✅ Catalog '{catalog}' ready")

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"  ✅ Schema '{catalog}.{schema}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Secrets Scope

# COMMAND ----------

scope = "riskbricks"
try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    if scope in scopes:
        print(f"✅ Secrets scope '{scope}' exists")
        try:
            dbutils.secrets.get(scope=scope, key="fred-api-key")
            print(f"  ✅ FRED API key found in secrets")
        except Exception:
            print(f"  ⚠️  FRED API key not set. Run:")
            print(f"     databricks secrets put-secret {scope} fred-api-key")
    else:
        print(f"⚠️  Secrets scope '{scope}' not found. Create it:")
        print(f"   databricks secrets create-scope {scope}")
        print(f"   databricks secrets put-secret {scope} fred-api-key")
except Exception as e:
    print(f"⚠️  Could not check secrets: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Model Endpoint

# COMMAND ----------

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    endpoints = [e.name for e in w.serving_endpoints.list()]
    target = "databricks-meta-llama-3-3-70b-instruct"
    if target in endpoints:
        print(f"✅ Model endpoint '{target}' available")
    else:
        print(f"⚠️  Model endpoint '{target}' not found")
        print(f"   Available endpoints: {', '.join(endpoints[:10])}")
        print(f"   Enable it via: Serving → Foundation Model APIs")
except Exception as e:
    print(f"⚠️  Could not check endpoints: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Config Auto-Detection

# COMMAND ----------

import sys, os
_nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = os.path.dirname(_nb_path)  # setup_workspace is at repo root
sys.path.insert(0, f"/Workspace{_repo_root}")

try:
    from config.riskbricks_config import cfg
    print(f"✅ Config auto-detected successfully:")
    print(f"   REPO_ROOT:   {cfg.REPO_ROOT}")
    print(f"   CATALOG:     {cfg.CATALOG}")
    print(f"   USER_EMAIL:  {cfg.USER_EMAIL}")
    print(f"   AGENTS_PATH: {cfg.AGENTS_PATH}")
    print(f"   DATA_PATH:   {cfg.DATA_PATH}")
except Exception as e:
    print(f"❌ Config import failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Next Steps
# MAGIC
# MAGIC 1. **Run data ingestion**: `notebooks/00_bronze/ingest_stocks_and_macros_data`
# MAGIC 2. **Run validation**: `notebooks/02_silver/validate_data_quality`
# MAGIC 3. **Run gold analytics**: `notebooks/03_gold/analytics/create_risk_analytics`
# MAGIC 4. **Create UC functions**: `notebooks/agent_framework/01_create_uc_tools`
# MAGIC 5. **Test agents**: `notebooks/agents/00_supervisor` (symbol=NVDA)
# MAGIC 6. **Deploy app**: `databricks bundle deploy --target dev`

# COMMAND ----------

print("\n" + "=" * 60)
print("🎉 WORKSPACE SETUP COMPLETE")
print("=" * 60)
print("\nRun the data pipeline in order:")
print("  1. notebooks/00_bronze/ingest_stocks_and_macros_data")
print("  2. notebooks/02_silver/validate_data_quality")
print("  3. notebooks/03_gold/analytics/create_risk_analytics")
print("  4. notebooks/agents/00_supervisor (symbol=NVDA)")
print("=" * 60)

dbutils.notebook.exit("success")
