# Databricks notebook source
# MAGIC %md
# MAGIC # Create & Test RiskBricks Agent
# MAGIC
# MAGIC Installs dependencies, loads the agent from `riskbricks_agent.py`,
# MAGIC runs test queries, and logs to MLflow.
# MAGIC
# MAGIC **Prerequisites:** Run `01_register_uc_tools` first.

# COMMAND ----------

# ── Import centralized config ────────────────────────────────────────
import sys, os
_nb  = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_root = "/Workspace" + (_nb[:_nb.find("/notebooks/")] if "/notebooks/" in _nb else os.path.dirname(_nb))
sys.path.insert(0, _root)
from config import CATALOG as _CFG_CATALOG
from config.riskbricks_config import cfg as _cfg


dbutils.widgets.text("catalog", _CFG_CATALOG)
catalog = dbutils.widgets.get("catalog").strip()
print(f"Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %pip install -U -qq databricks-langchain unitycatalog-langchain[databricks] mlflow>=2.13.1 databricks-agents langchain langchain-community

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
from riskbricks_agent import (
    multi_agent_graph as agent,
    RISK_TOOLS, PRICE_TARGET_TOOLS, FACTOR_TOOLS,
    DECISION_TOOLS, NEWS_TOOLS, ML_DIRECTION_TOOLS,
)

tools = RISK_TOOLS + PRICE_TARGET_TOOLS + FACTOR_TOOLS + DECISION_TOOLS + NEWS_TOOLS + ML_DIRECTION_TOOLS

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
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

# Set experiment
experiment_name = "/Users/" + spark.sql("SELECT current_user()").first()[0] + "/riskbricks_agent"
mlflow.set_experiment(experiment_name)

input_example = {
    "messages": [{"role": "user", "content": "What is the portfolio risk for Sarah Russel?"}]
}

# Declare all resource dependencies for automatic authentication passthrough
# This ensures the system service principal gets the right UC grants automatically
resources = [
    DatabricksServingEndpoint(endpoint_name=_cfg.LLM_ENDPOINT),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_portfolio_risk_metrics"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_stress_test_results"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_portfolio_holdings"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_sector_exposures"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_factor_exposures"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_stock_forecast"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_decision_signal"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_macro_context"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_news_context"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_ml_stock_forecast"),
    DatabricksFunction(function_name=f"{catalog}.agent_tools.get_ml_market_overview"),
]

with mlflow.start_run(run_name="riskbricks_agent_ml_direction") as run:
    model_info = mlflow.langchain.log_model(
        lc_model=os.path.join(f"/Workspace{_agents_dir}", "riskbricks_agent.py"),
        name="riskbricks_agent",
        input_example=input_example,
        resources=resources,
    )
    print(f"✅ Agent logged to MLflow (with {len(resources)} resource dependencies)")
    print(f"   Run ID:    {run.info.run_id}")
    print(f"   Model URI: {model_info.model_uri}")

# COMMAND ----------

# DBTITLE 1,Verify loaded model
# Verify the logged model loads correctly
# Note: RiskBricksSupervisor is a ChatAgent — local predict() doesn't work
# through the langchain loader. Full prediction test happens on the serving
# endpoint (cell 21). Here we verify the artifact metadata.
model_metadata = mlflow.models.get_model_info(model_info.model_uri)

print(f"✅ Model artifact verified")
print(f"   Flavors:   {list(model_metadata.flavors.keys())}")
print(f"   Signature: {model_metadata.signature}")
print(f"   Model URI: {model_info.model_uri}")
print(f"   Run ID:    {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog

# COMMAND ----------

uc_model_name = f"{catalog}.agents.riskbricks_agent"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.agents")

# UC requires a model signature — set it on the already-logged model
from mlflow.models import ModelSignature
try:
    from mlflow.types.llm import ChatParams, ChatCompletionResponse
    signature = ModelSignature(inputs=ChatParams(), outputs=ChatCompletionResponse())
except Exception:
    from mlflow.models.signature import infer_signature
    signature = infer_signature(
        {"messages": [{"role": "user", "content": "test"}]},
        {"choices": [{"index": 0, "message": {"role": "assistant", "content": "test"}, "finish_reason": "stop"}]},
    )
mlflow.models.set_signature(model_info.model_uri, signature)

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

# Final summary — deployment is handled by 03_deploy_agent
print(f"""
{'='*60}
✅ AGENT CREATION COMPLETE
{'='*60}

Model:     {uc_model_name} v{uc_model_info.version}
Run ID:    {run.info.run_id}

Next Step: Run 03_deploy_agent to deploy the serving endpoint.
{'='*60}
""")
