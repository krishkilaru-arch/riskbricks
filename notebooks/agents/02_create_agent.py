# Databricks notebook source
# MAGIC %md
# MAGIC # Create & Test RiskBricks Agent
# MAGIC
# MAGIC Installs dependencies, loads the agent from `riskbricks_agent.py`,
# MAGIC runs test queries, and logs to MLflow.
# MAGIC
# MAGIC **Prerequisites:** Run `01_register_uc_tools` first.

# COMMAND ----------

# MAGIC %pip install -U -qq databricks-langchain unitycatalog-langchain[databricks] mlflow>=2.13.1 databricks-agents langchain langchain-community
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Check langchain version and imports
import mlflow
mlflow.langchain.autolog()
mlflow.set_registry_uri("databricks-uc")

print(f"✅ MLflow tracking: {mlflow.get_tracking_uri()}")
print(f"✅ MLflow registry: {mlflow.get_registry_uri()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Agent from Code

# COMMAND ----------

# DBTITLE 1,Load agent from code
import os, sys

# Resolve path to agent definition
_nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_agents_dir = os.path.dirname(_nb_path)
_agent_file = f"/Workspace{_agents_dir}/riskbricks_agent.py"

print(f"📂 Agent definition: {_agent_file}")
assert os.path.exists(_agent_file), f"Agent file not found: {_agent_file}"

# Import the agent
sys.path.insert(0, f"/Workspace{_agents_dir}")
from riskbricks_agent import agent, tools

print(f"✅ Agent loaded with {len(tools)} tools:")
for t in tools:
    print(f"   • {t.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Queries

# COMMAND ----------

# DBTITLE 1,Test query helper
def test_query(query: str):
    """Run a test query and print the result."""
    print(f"\n{'='*70}")
    print(f"🧑 {query}")
    print(f"{'='*70}")
    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": query}]}
        )
        # Extract the last AI message
        messages = result.get("messages", result) if isinstance(result, dict) else result
        if isinstance(messages, dict) and "messages" in messages:
            messages = messages["messages"]
        if isinstance(messages, list):
            ai_msgs = [m for m in messages if hasattr(m, 'type') and m.type == 'ai' and m.content]
            if ai_msgs:
                print(f"\n🤖 {ai_msgs[-1].content}")
            else:
                print(f"\n🤖 {messages[-1]}")
        else:
            print(f"\n🤖 {result}")
        return result
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

# COMMAND ----------

# Test 1: Basic risk query
test_query("What is Sarah Russel's portfolio risk? Show her VaR and beta.")

# COMMAND ----------

# Test 2: Cross-manager comparison
test_query("Compare all three managers' stress test results. Who is most vulnerable to a market crash?")

# COMMAND ----------

# Test 3: Stock-level analysis
test_query("Give me the forecast and decision signal for NVDA.")

# COMMAND ----------

# Test 4: Macro context
test_query("What's the current macro environment? How might rising rates affect our portfolios?")

# COMMAND ----------

# Test 5: Comprehensive analysis
test_query("Give me a complete risk report for Mohit Arora — holdings, sector exposure, stress tests, and recommendations.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Agent to MLflow

# COMMAND ----------

# DBTITLE 1,Log agent to MLflow
import mlflow

# Set experiment
experiment_name = "/Users/" + spark.sql("SELECT current_user()").first()[0] + "/riskbricks_agent"
mlflow.set_experiment(experiment_name)

input_example = {
    "messages": [{"role": "user", "content": "What is the portfolio risk for Sarah Russel?"}]
}

with mlflow.start_run(run_name="riskbricks_agent_v1") as run:
    model_info = mlflow.langchain.log_model(
        lc_model=os.path.join(f"/Workspace{_agents_dir}", "riskbricks_agent.py"),
        name="riskbricks_agent",
        input_example=input_example,
    )
    print(f"✅ Agent logged to MLflow")
    print(f"   Run ID:    {run.info.run_id}")
    print(f"   Model URI: {model_info.model_uri}")

# COMMAND ----------

# DBTITLE 1,Verify loaded model
# Verify the logged model loads correctly
loaded_agent = mlflow.langchain.load_model(model_info.model_uri)
result = loaded_agent.invoke(
    {"messages": [{"role": "user", "content": "How many managers do we have and what are their risk profiles?"}]}
)
messages = result.get("messages", []) if isinstance(result, dict) else []
ai_msgs = [m for m in messages if hasattr(m, 'type') and m.type == 'ai' and m.content]
if ai_msgs:
    print(f"\n✅ Loaded model test passed")
    print(f"   Response: {ai_msgs[-1].content[:300]}...")
else:
    print(f"✅ Model loaded, response: {str(result)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog

# COMMAND ----------

uc_model_name = "riskbricks.agents.riskbricks_agent"

# Ensure schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS riskbricks.agents")

uc_model_info = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=uc_model_name,
)

print(f"✅ Registered in Unity Catalog")
print(f"   Model:   {uc_model_name}")
print(f"   Version: {uc_model_info.version}")

# COMMAND ----------

# Save version info for deployment notebook
dbutils.notebook.exit(f'{{"model_name": "{uc_model_name}", "version": "{uc_model_info.version}", "run_id": "{run.info.run_id}"}}')

# COMMAND ----------

# DBTITLE 1,Deploy header
# MAGIC %md
# MAGIC ## Deploy to Serving Endpoint

# COMMAND ----------

# DBTITLE 1,Deploy agent to serving endpoint
# Re-log with explicit ChatCompletion signature for agents.deploy() compatibility
from mlflow.models import ModelSignature

try:
    from mlflow.types.llm import ChatParams, ChatCompletionResponse
    signature = ModelSignature(
        inputs=ChatParams(),
        outputs=ChatCompletionResponse(),
    )
    print("✅ Using ChatParams/ChatCompletionResponse signature")
except Exception:
    # Fallback for older MLflow versions
    from mlflow.models.signature import infer_signature
    input_ex = {"messages": [{"role": "user", "content": "test"}]}
    output_ex = {
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "test"}, "finish_reason": "stop"}]
    }
    signature = infer_signature(input_ex, output_ex)
    print("✅ Using inferred signature (fallback)")

# Re-log with proper signature
with mlflow.start_run(run_name="riskbricks_agent_v2_deploy") as run2:
    model_info2 = mlflow.langchain.log_model(
        lc_model=os.path.join(f"/Workspace{_agents_dir}", "riskbricks_agent.py"),
        name="riskbricks_agent",
        input_example={"messages": [{"role": "user", "content": "What is Sarah Russel's portfolio risk?"}]},
        signature=signature,
    )
    print(f"✅ Re-logged with ChatCompletion signature")
    print(f"   Run ID:    {run2.info.run_id}")
    print(f"   Model URI: {model_info2.model_uri}")

# Re-register new version
uc_model_info2 = mlflow.register_model(
    model_uri=model_info2.model_uri,
    name=uc_model_name,
)
print(f"✅ Registered v{uc_model_info2.version} in UC")

# Deploy
from databricks import agents

deployment = agents.deploy(
    uc_model_name,
    uc_model_info2.version,
)

print(f"\n✅ Agent deployed successfully!")
print(f"   Endpoint:   {deployment.endpoint_name}")
print(f"   Query URL:  {deployment.query_endpoint}")

# COMMAND ----------

# DBTITLE 1,Test the deployed endpoint
import requests, json

# Get workspace URL and token
ctx = dbutils.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

print(f"Testing endpoint: {deployment.endpoint_name}")
print(f"Note: endpoint may take 5-10 min to become ready after initial deployment.\n")

response = requests.post(
    f"{host}/serving-endpoints/{deployment.endpoint_name}/invocations",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    },
    json={
        "messages": [
            {"role": "user", "content": "Compare portfolio risk for all three managers. Show VaR, beta, and volatility."}
        ]
    },
    timeout=120,
)

if response.status_code == 200:
    result = response.json()
    print("✅ Endpoint responded successfully:")
    if "choices" in result:
        print(result["choices"][0]["message"]["content"])
    else:
        print(json.dumps(result, indent=2)[:1500])
else:
    print(f"⚠️ Status {response.status_code}")
    print(f"   {response.text[:500]}")
    print(f"\n💡 If 'NOT_READY', wait a few minutes and re-run this cell.")

# COMMAND ----------

# DBTITLE 1,Print endpoint info
host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get()

print(f"""
{'='*60}
🚀 RISKBRICKS AGENT DEPLOYMENT COMPLETE
{'='*60}

Endpoint Name:  {deployment.endpoint_name}
Query URL:      {deployment.query_endpoint}

Review App: Check the MLflow experiment for the Review App link.

Usage (Python SDK):
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  response = w.serving_endpoints.query(
      name="{deployment.endpoint_name}",
      messages=[{{"role": "user", "content": "your question"}}]
  )

Usage (REST API):
  curl -X POST {host}/serving-endpoints/{deployment.endpoint_name}/invocations \\
    -H "Authorization: Bearer $TOKEN" \\
    -H "Content-Type: application/json" \\
    -d '{{"messages": [{{"role": "user", "content": "your question"}}]}}'
{'='*60}
""")
