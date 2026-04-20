# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# DBTITLE 1,Header
dbutils.widgets.text("catalog", "riskbricks")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install -U -qq databricks-agents>=0.16.0 mlflow>=2.20.2 langgraph databricks-langchain typing_extensions>=4.12
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,MLflow setup
import mlflow
mlflow.set_registry_uri("databricks-uc")
print("✅ MLflow registry ready")

# COMMAND ----------

# DBTITLE 1,Get latest model version
UC_MODEL_NAME = f"{catalog}.agents.riskbricks_agent"

from mlflow import MlflowClient
client = MlflowClient()
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)

print(f"📦 Model:   {UC_MODEL_NAME}")
print(f"📦 Version: {latest_version}")

# COMMAND ----------

# DBTITLE 1,Cleanup stale deployments and endpoints
from databricks import agents
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# --- Phase 1: Delete all old agent deployments ---
print("\n🧹 Phase 1: Cleaning up agent deployments...")
all_deployments = agents.list_deployments()
for d in all_deployments:
    if d.model_name == UC_MODEL_NAME:
        print(f"  Deleting deployment: v{d.model_version} on {d.endpoint_name}")
        try:
            agents.delete_deployment(d.model_name, d.model_version)
        except Exception as e:
            print(f"    ⚠️  Skipped (may already be deleted): {e}")

print("\n🧹 Phase 2: Deleting stale serving endpoints...")
stale_endpoints = [
    "agents_riskbricks-agents-riskbricks_agent",
    "riskbricks-agent-ml",
    "riskbricks-agent-v3",
    "riskbricks-agent-v2",
]
for ep in stale_endpoints:
    try:
        w.serving_endpoints.delete(name=ep)
        print(f"  ✅ Deleted endpoint: {ep}")
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "404" in str(e):
            print(f"  —  Already gone: {ep}")
        else:
            print(f"  ⚠️  Error deleting {ep}: {e}")

print("\n✅ Cleanup complete. Ready for fresh deployment.")

# COMMAND ----------

# DBTITLE 1,Re-log model with langchain flavor and register
import os, mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")

# __file__ not available in interactive mode; resolve from notebook path
try:
    agent_file = os.path.join(os.path.dirname(__file__), "riskbricks_agent.py")
except NameError:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _ws_dir = "/Workspace" + os.path.dirname(_nb_path)
    agent_file = os.path.join(_ws_dir, "riskbricks_agent.py")

print(f"Agent file: {agent_file}")
assert os.path.exists(agent_file), f"Agent file not found: {agent_file}"

# LLM endpoint name (matches the inline default in riskbricks_agent.py)
_LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# Declare all dependent resources for governance
resources = [
    DatabricksServingEndpoint(endpoint_name=_LLM_ENDPOINT),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_portfolio_risk_metrics"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_stress_test_results"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_portfolio_holdings"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_sector_exposures"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_macro_context"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_stock_forecast"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_decision_signal"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_factor_exposures"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_news_context"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_ml_stock_forecast"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_ml_market_overview"),
]

input_example = {"messages": [{"role": "user", "content": "What is Mohit Arora's portfolio risk?"}]}

# Explicit pip requirements — excludes delta-spark which is a
# Databricks-internal build (v3.4.0 doesn't exist on public PyPI)
# and is NOT needed at serving time (agent uses UC functions, not Spark)
_pip_requirements = [
    "mlflow>=3.11",
    "langgraph",
    "langchain-core",
    "databricks-langchain>=0.18",
    "databricks-agents>=1.9",
    "databricks-connect>=16.4",
    "unitycatalog-ai[databricks]",
    "cloudpickle>=3.0",
    "aiohttp>=3.11",
    "typing_extensions>=4.12",
    "cryptography>=44.0",
]

with mlflow.start_run(run_name="riskbricks_supervisor_clean_deploy") as run:
    model_info = mlflow.pyfunc.log_model(
        python_model=agent_file,
        name="riskbricks_agent",
        input_example=input_example,
        resources=resources,
        pip_requirements=_pip_requirements,
    )
    print(f"\u2705 Model logged (pyfunc + ChatAgent wrapper)")
    print(f"   Run ID:    {run.info.run_id}")
    print(f"   Model URI: {model_info.model_uri}")
    print(f"   Pip reqs:  {len(_pip_requirements)} packages (delta-spark excluded)")

# Register to Unity Catalog
from mlflow import MlflowClient
client = MlflowClient()
uc_info = client.create_model_version(
    name=UC_MODEL_NAME,
    source=model_info.model_uri,
    run_id=run.info.run_id,
)
latest_version = int(uc_info.version)
print(f"\n\u2705 Registered as {UC_MODEL_NAME} v{latest_version}")

# COMMAND ----------

# DBTITLE 1,Deploy to serving endpoint
from databricks import agents
from databricks.sdk import WorkspaceClient

ENDPOINT_NAME = "riskbricks-supervisor-agent"
w = WorkspaceClient()

# Delete stale endpoint (left in broken state by failed v19 deployment)
try:
    w.serving_endpoints.delete(name=ENDPOINT_NAME)
    print(f"Deleted stale endpoint '{ENDPOINT_NAME}'")
    import time; time.sleep(10)  # brief pause for cleanup
except Exception as e:
    print(f"Endpoint cleanup: {e}")

print(f"Deploying {UC_MODEL_NAME} v{latest_version} to '{ENDPOINT_NAME}'...")

deployment = agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=latest_version,
    endpoint_name=ENDPOINT_NAME,
    scale_to_zero=False,
)

print(f"Deployment initiated!")
print(f"   Model:    {UC_MODEL_NAME} v{latest_version}")
print(f"   Endpoint: {ENDPOINT_NAME}")
print(f"   Status:   Creating fresh endpoint (10-15 min)...")

# COMMAND ----------

# DBTITLE 1,Test the deployed endpoint
import requests, json, time

ENDPOINT_NAME = "riskbricks-supervisor-agent"

ctx = dbutils.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

print(f"Waiting for endpoint '{ENDPOINT_NAME}' to become ready...")
for i in range(40):
    resp = requests.get(
        f"{host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
        headers={"Authorization": f"Bearer {token}"},
    )
    if resp.status_code == 200:
        state = resp.json().get("state", {})
        ready = state.get("ready", "UNKNOWN")
        config = state.get("config_update", "UNKNOWN")
        print(f"  [{i*30:>4}s] ready={ready}, config={config}")
        if ready == "READY" and config == "NOT_UPDATING":
            print(f"\nEndpoint READY!")
            break
        if "FAILED" in str(config):
            print(f"\nDeployment FAILED. Check service logs.")
            break
    elif resp.status_code == 404:
        print(f"  [{i*30:>4}s] endpoint not yet created...")
    time.sleep(30)
else:
    print("\nStill not ready after 20 min. Try again later.")

# Test query
print(f"\nTesting with a sample query...")
response = requests.post(
    f"{host}/serving-endpoints/{ENDPOINT_NAME}/invocations",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"messages": [{"role": "user", "content": "What is the VaR for Mohit Arora's portfolio?"}]},
    timeout=180,
)

if response.status_code == 200:
    result = response.json()
    if "choices" in result:
        print("Response:\n")
        print(result["choices"][0]["message"]["content"])
    elif "messages" in result:
        ai_msgs = [m for m in result["messages"] if m.get("role") == "assistant" and m.get("content")]
        if ai_msgs:
            print("Response:\n")
            print(ai_msgs[-1]["content"])
        else:
            print(json.dumps(result, indent=2)[:2000])
    else:
        print(json.dumps(result, indent=2)[:2000])
else:
    print(f"Status {response.status_code}: {response.text[:500]}")

# COMMAND ----------

# DBTITLE 1,Deployment info summary
print(f"""
{'='*60}
🚀 RISKBRICKS AGENT DEPLOYMENT COMPLETE
{'='*60}

Endpoint:  {ENDPOINT_NAME}
Model:     {UC_MODEL_NAME} v{latest_version}

Usage (Python):
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  r = w.serving_endpoints.query(
      name="{ENDPOINT_NAME}",
      messages=[{{"role": "user", "content": "your question"}}]
  )

Streamlit App:
  Set RISKBRICKS_AGENT_ENDPOINT={ENDPOINT_NAME}
{'='*60}
""")
