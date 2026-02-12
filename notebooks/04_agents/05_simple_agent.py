# Databricks notebook source
# MAGIC %md
# MAGIC # Simple RiskBricks Agent
# MAGIC 
# MAGIC This notebook creates a simple agent that can answer questions about portfolio managers.

# COMMAND ----------

# MAGIC %pip install mlflow langchain langchain-community databricks-langchain databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Simple Agent Model

# COMMAND ----------

class SimpleRiskBricksAgent(mlflow.pyfunc.PythonModel):
    """
    Simple agent that queries Unity Catalog functions to answer questions.
    """
    
    def load_context(self, context):
        """Initialize the agent with Spark session."""
        from pyspark.sql import SparkSession
        self.spark = SparkSession.builder.getOrCreate()
    
    def predict(self, context, model_input):
        """
        Process user queries and return responses.
        
        Args:
            model_input: DataFrame with 'input' column containing user queries
        """
        import json
        
        # Extract the query
        if isinstance(model_input, pd.DataFrame):
            query = model_input.iloc[0]['input'] if 'input' in model_input.columns else str(model_input.iloc[0, 0])
        else:
            query = str(model_input)
        
        query_lower = query.lower()
        
        try:
            # Route queries to appropriate UC functions
            
            # Portfolio Manager queries
            if 'sarah' in query_lower and 'risk' in query_lower:
                result = self.spark.sql("""
                    SELECT * FROM riskbricks.agent_tools.get_portfolio_holdings('Sarah Russel')
                """).collect()
                
                risk_metrics = self.spark.sql("""
                    SELECT * FROM riskbricks.agent_tools.get_risk_metrics('Sarah Russel')
                """).collect()
                
                if risk_metrics:
                    rm = risk_metrics[0]
                    response = f"""Sarah Russel is a Conservative portfolio manager with:
- Total AUM: ${rm.aum_usd:,.0f}
- Portfolio Beta: {rm.portfolio_beta:.2f}
- Volatility: {rm.weighted_volatility_pct:.2f}%
- 1-Day VaR (95%): ${rm.var_1day_95_usd:,.0f}
- Number of positions: {rm.num_positions}

Her portfolio focuses on low-risk, stable companies with defensive characteristics."""
                    return [response]
            
            # Portfolio holdings
            elif 'holdings' in query_lower or 'positions' in query_lower:
                for name in ['sarah russel', 'rena tang', 'mohit arora']:
                    if name in query_lower:
                        result = self.spark.sql(f"""
                            SELECT * FROM riskbricks.agent_tools.get_portfolio_holdings('{name.title()}')
                        """).collect()
                        
                        if result:
                            holdings_summary = "\n".join([f"- {r.symbol}: ${r.value_usd:,.0f} ({r.weight_pct:.1f}%)" for r in result[:10]])
                            response = f"""{name.title()}'s Top Holdings:

{holdings_summary}

{'...' if len(result) > 10 else ''}
Total positions: {len(result)}"""
                            return [response]
            
            # Sector exposure
            elif 'sector' in query_lower or 'exposure' in query_lower:
                for name in ['sarah russel', 'rena tang', 'mohit arora']:
                    if name in query_lower:
                        result = self.spark.sql(f"""
                            SELECT * FROM riskbricks.agent_tools.get_sector_exposures('{name.title()}')
                        """).collect()
                        
                        if result:
                            sector_summary = "\n".join([f"- {r.sector}: {r.sector_weight_pct:.1f}%" for r in result])
                            response = f"""{name.title()}'s Sector Exposure:

{sector_summary}"""
                            return [response]
            
            # Stress tests
            elif 'stress' in query_lower or 'scenario' in query_lower:
                for name in ['sarah russel', 'rena tang', 'mohit arora']:
                    if name in query_lower:
                        result = self.spark.sql(f"""
                            SELECT * FROM riskbricks.agent_tools.get_stress_tests('{name.title()}')
                        """).collect()
                        
                        if result:
                            stress_summary = "\n".join([f"- {r.scenario_name}: {r.impact_pct:.1f}% (${r.impact_usd:,.0f})" for r in result])
                            response = f"""{name.title()}'s Stress Test Results:

{stress_summary}"""
                            return [response]
            
            # Compare managers
            elif 'compare' in query_lower or 'all managers' in query_lower or 'three' in query_lower:
                result = self.spark.sql("""
                    SELECT * FROM riskbricks.agent_tools.compare_managers()
                """).collect()
                
                if result:
                    comparison = "\n\n".join([
                        f"""**{r.manager_name}** ({r.risk_profile}):
- AUM: ${r.aum_usd:,.0f}
- Beta: {r.portfolio_beta:.2f}
- VaR (1-day, 95%): ${r.var_1day_95_usd:,.0f}
- Holdings: {r.num_holdings}
- Target Return: {r.target_return_pct:.0f}%
- Max Volatility: {r.max_volatility_pct:.0f}%"""
                        for r in result
                    ])
                    response = f"""Portfolio Manager Comparison:

{comparison}

Total AUM across all managers: ${sum(r.aum_usd for r in result):,.0f}"""
                    return [response]
            
            # Macro context
            elif 'macro' in query_lower or 'economic' in query_lower or 'market' in query_lower:
                result = self.spark.sql("""
                    SELECT * FROM riskbricks.agent_tools.get_macro_context()
                """).collect()
                
                if result:
                    macro_summary = "\n".join([f"- {r.indicator_name}: {r.latest_value:.2f} (as of {r.latest_date})" for r in result])
                    response = f"""Current Macroeconomic Context:

{macro_summary}"""
                    return [response]
            
            # Default response with suggestions
            else:
                return [f"""I can help you with:

1. Portfolio manager risk profiles (e.g., "What is Sarah Russel's risk profile?")
2. Portfolio holdings (e.g., "Show me Mohit Arora's holdings")
3. Sector exposures (e.g., "What is Rena Tang's sector exposure?")
4. Stress test results (e.g., "Show stress tests for Sarah Russel")
5. Manager comparisons (e.g., "Compare all three managers")
6. Macroeconomic indicators (e.g., "What's the current macro context?")

Your query: "{query}"

Please ask a specific question about our portfolio managers: Sarah Russel, Rena Tang, or Mohit Arora."""]
        
        except Exception as e:
            return [f"Error processing query: {str(e)}\n\nPlease try asking about Sarah Russel, Rena Tang, or Mohit Arora's portfolios."]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Agent

# COMMAND ----------

# Create a sample input for signature inference
sample_input = pd.DataFrame({
    "input": ["What is Sarah Russel's risk profile?"]
})

# Create experiment
mlflow.set_experiment("/Shared/RiskBricks/simple_agent")

with mlflow.start_run(run_name="simple_riskbricks_agent") as run:
    
    # Log the model
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=SimpleRiskBricksAgent(),
        input_example=sample_input,
        signature=mlflow.models.infer_signature(
            sample_input,
            ["Sarah Russel is a Conservative portfolio manager..."]
        ),
        pip_requirements=[
            "mlflow>=2.10.0",
            "databricks-sdk>=0.12.0"
        ]
    )
    
    print(f"✅ Model logged successfully!")
    print(f"🔗 Run ID: {run.info.run_id}")
    print(f"📦 Model URI: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register to Unity Catalog

# COMMAND ----------

# Register the model to Unity Catalog
model_name = "riskbricks.agents.riskbricks_supervisor"

# Get the latest run
client = mlflow.tracking.MlflowClient()
experiment = client.get_experiment_by_name("/Shared/RiskBricks/simple_agent")
runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["start_time DESC"],
    max_results=1
)

latest_run = runs[0]
model_uri = f"runs:/{latest_run.info.run_id}/agent"

# Register the model
model_version = mlflow.register_model(
    model_uri=model_uri,
    name=model_name,
    tags={
        "type": "simple_agent",
        "version": "2.0"
    }
)

print(f"✅ Model registered to Unity Catalog!")
print(f"📦 Model: {model_name}")
print(f"🔢 Version: {model_version.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Agent

# COMMAND ----------

# Load the model
loaded_model = mlflow.pyfunc.load_model(f"models:/{model_name}/{model_version.version}")

# Test queries
test_queries = [
    "What is Sarah Russel's risk profile?",
    "Compare all three managers",
    "Show me Mohit Arora's sector exposure"
]

print("="*80)
print("🧪 TESTING SIMPLE AGENT")
print("="*80)

for query in test_queries:
    print(f"\n❓ Query: {query}")
    print("-" * 80)
    
    test_input = pd.DataFrame({"input": [query]})
    response = loaded_model.predict(test_input)
    
    print(response[0])
    print()

print("="*80)
print("✅ Testing complete!")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Serving Endpoint
# MAGIC 
# MAGIC Run this to update the serving endpoint with the new model:
# MAGIC 
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC 
# MAGIC w = WorkspaceClient()
# MAGIC 
# MAGIC w.serving_endpoints.update_config(
# MAGIC     name="riskbricks-agent-endpoint",
# MAGIC     served_entities=[{
# MAGIC         "entity_name": "riskbricks.agents.riskbricks_supervisor",
# MAGIC         "entity_version": "2",
# MAGIC         "workload_size": "Small",
# MAGIC         "scale_to_zero_enabled": True
# MAGIC     }]
# MAGIC )
# MAGIC 
# MAGIC print("✅ Serving endpoint updated!")
# MAGIC ```

# COMMAND ----------

print(f"""
🎉 SIMPLE AGENT DEPLOYED!

Next Steps:
1. Update the serving endpoint to use version {model_version.version}
2. Test the endpoint from the Streamlit app
3. Try these queries:
   - "What is Sarah Russel's risk profile?"
   - "Compare all three managers"
   - "Show me Mohit Arora's holdings"

Model Details:
- Name: {model_name}
- Version: {model_version.version}
- Run ID: {latest_run.info.run_id}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

