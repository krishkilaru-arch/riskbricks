# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# DBTITLE 1,Header
# MAGIC %md
# MAGIC # Deploy RiskBricks Agent
# MAGIC
# MAGIC Deploys the registered agent to a Model Serving endpoint using `databricks.agents.deploy()`.
# MAGIC
# MAGIC **Prerequisites:** Run `02_create_agent` first to log and register the model.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install -U -qq databricks-agents>=0.12.0 mlflow>=2.13.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,MLflow setup
import mlflow
mlflow.set_registry_uri("databricks-uc")
print("✅ MLflow registry ready")

# COMMAND ----------

# DBTITLE 1,Get latest model version
UC_MODEL_NAME = "riskbricks.agents.riskbricks_agent"

from mlflow import MlflowClient
client = MlflowClient()
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)

print(f"📦 Model:   {UC_MODEL_NAME}")
print(f"📦 Version: {latest_version}")

# COMMAND ----------

# DBTITLE 1,Deploy to serving endpoint
from databricks import agents
import mlflow

# Set experiment to a non-Git path for tracing
experiment_name = "/Users/" + spark.sql("SELECT current_user()").first()[0] + "/riskbricks_agent"
mlflow.set_experiment(experiment_name)

try:
    deployment = agents.deploy(
        UC_MODEL_NAME,
        latest_version,
    )
    print(f"\n✅ Agent deployed successfully!")
    print(f"   Endpoint:  {deployment.endpoint_name}")
    print(f"   Query URL: {deployment.query_endpoint}")
except ValueError as e:
    if "already serves model" in str(e):
        print(f"ℹ️  Endpoint already serving {UC_MODEL_NAME} v{latest_version}")
        # Retrieve existing deployment info
        deployments = agents.list_deployments()
        deployment = [d for d in deployments if d.model_name == UC_MODEL_NAME][0]
        print(f"   Endpoint:  {deployment.endpoint_name}")
        print(f"   Query URL: {deployment.query_endpoint}")
    else:
        raise

# COMMAND ----------

# DBTITLE 1,Test the deployed endpoint
import requests, json

ctx = dbutils.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

response = requests.post(
    f"{host}/serving-endpoints/{deployment.endpoint_name}/invocations",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"messages": [{"role": "user", "content": "Compare portfolio risk for all three managers. Show VaR, beta, and volatility."}]},
    timeout=120,
)

if response.status_code == 200:
    result = response.json()
    # ChatCompletion format
    if "choices" in result:
        print("✅ Endpoint response:\n")
        print(result["choices"][0]["message"]["content"])
    # LangGraph messages format
    elif "messages" in result:
        ai_msgs = [m for m in result["messages"] if m.get("type") == "ai" and m.get("content")]
        if ai_msgs:
            print("✅ Endpoint response:\n")
            print(ai_msgs[-1]["content"])
        else:
            print("⚠️ No AI response with content found")
            print(json.dumps(result, indent=2)[:1500])
    else:
        print(json.dumps(result, indent=2)[:1500])
else:
    print(f"⚠️ Status {response.status_code}: {response.text[:500]}")
    print("\nNote: Endpoint may take 5-10 min to warm up after deployment.")

# COMMAND ----------

# DBTITLE 1,Deployment info summary
print(f"""
{'='*60}
🚀 RISKBRICKS AGENT DEPLOYMENT COMPLETE
{'='*60}

Endpoint:  {deployment.endpoint_name}
Query URL: {deployment.query_endpoint}
Model:     {UC_MODEL_NAME} v{latest_version}

Usage (Python):
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  r = w.serving_endpoints.query(
      name="{deployment.endpoint_name}",
      messages=[{{"role": "user", "content": "your question"}}]
  )

Usage (REST):
  curl -X POST {host}/serving-endpoints/{deployment.endpoint_name}/invocations \\
    -H "Authorization: Bearer $TOKEN" \\
    -H "Content-Type: application/json" \\
    -d '{{"messages": [{{"role": "user", "content": "your question"}}]}}'
{'='*60}
""")

# COMMAND ----------

# DBTITLE 1,Step 2a: Install dependencies
# MAGIC %pip install -U -qq databricks-agents>=0.16.0 mlflow>=2.20.2 langgraph databricks-langchain
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Step 2b: Re-log with resources and register
import os, mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = "riskbricks.agents.riskbricks_agent"
agents_dir = "/Workspace/Users/krish.kilaru@lumenalta.com/vibe_coding/riskbricks/notebooks/agents"

experiment_name = "/Users/" + spark.sql("SELECT current_user()").first()[0] + "/riskbricks_agent"
mlflow.set_experiment(experiment_name)

resources = [
    DatabricksServingEndpoint(endpoint_name="databricks-meta-llama-3-3-70b-instruct"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_portfolio_risk_metrics"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_stress_test_results"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_portfolio_holdings"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_sector_exposures"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_macro_context"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_stock_forecast"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_decision_signal"),
    DatabricksFunction(function_name="riskbricks.agent_tools.get_factor_exposures"),
]

input_example = {"messages": [{"role": "user", "content": "What is the VaR for Mohit Arora?"}]}

with mlflow.start_run(run_name="riskbricks_agent_v5_with_resources") as run:
    model_info = mlflow.pyfunc.log_model(
        python_model=os.path.join(agents_dir, "riskbricks_agent.py"),
        name="riskbricks_agent",
        input_example=input_example,
        resources=resources,
    )
    print(f"Run ID: {run.info.run_id}")
    print(f"Model URI: {model_info.model_uri}")

from mlflow import MlflowClient
client = MlflowClient()
uc_info = client.create_model_version(
    name=UC_MODEL_NAME,
    source=model_info.model_uri,
    run_id=run.info.run_id,
)
print(f"\n✅ Registered v{uc_info.version} in UC")

# COMMAND ----------

# DBTITLE 1,Step 3: Deploy via agents.deploy()
from databricks import agents
import mlflow

mlflow.set_registry_uri("databricks-uc")
experiment_name = "/Users/" + spark.sql("SELECT current_user()").first()[0] + "/riskbricks_agent"
mlflow.set_experiment(experiment_name)

new_version = int(uc_info.version)
print(f"🚀 Deploying v{new_version} via agents.deploy()...")
deployment = agents.deploy("riskbricks.agents.riskbricks_agent", new_version)
print(f"\n✅ Deployed!")
print(f"   Endpoint:  {deployment.endpoint_name}")
print(f"   Query URL: {deployment.query_endpoint}")

# COMMAND ----------

# DBTITLE 1,Step 4: Poll until ready and test
import time, requests, json

ctx = dbutils.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
endpoint_name = deployment.endpoint_name

print(f"⏳ Waiting for endpoint to become ready...")
for i in range(40):
    resp = requests.get(
        f"{host}/api/2.0/serving-endpoints/{endpoint_name}",
        headers={"Authorization": f"Bearer {token}"},
    )
    if resp.status_code == 200:
        state = resp.json().get("state", {})
        ready = state.get("ready", "UNKNOWN")
        config = state.get("config_update", "UNKNOWN")
        print(f"  [{i*30:>4}s] ready={ready}, config={config}")
        if ready == "READY" and config == "NOT_UPDATING":
            print(f"\n✅ Endpoint READY!")
            break
        if "FAILED" in str(config):
            print(f"\n❌ Deployment FAILED. Check service logs.")
            break
    time.sleep(30)
else:
    print("\n⚠️ Still not ready after 20 min.")

# Test with VaR question
print(f"\n🔍 Testing: What is the VaR for Mohit Arora's portfolio?")
response = requests.post(
    f"{host}/serving-endpoints/{endpoint_name}/invocations",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"messages": [{"role": "user", "content": "What is the VaR for Mohit Arora's portfolio?"}]},
    timeout=120,
)
if response.status_code == 200:
    result = response.json()
    if "choices" in result:
        print("\n✅ Response:\n")
        print(result["choices"][0]["message"]["content"])
    elif "messages" in result:
        ai_msgs = [m for m in result["messages"] if m.get("role") == "assistant" and m.get("content")]
        if ai_msgs:
            print("\n✅ Response:\n")
            print(ai_msgs[-1]["content"])
        else:
            print(json.dumps(result, indent=2)[:2000])
    else:
        print(json.dumps(result, indent=2)[:2000])
else:
    print(f"\n⚠️ Status {response.status_code}: {response.text[:500]}")
