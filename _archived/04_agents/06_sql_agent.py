# Databricks notebook source
# MAGIC %md
# MAGIC # SQL-Based RiskBricks Agent (No Spark)
# MAGIC
# MAGIC This notebook creates an agent that uses Databricks SQL connector instead of Spark.

# COMMAND ----------

# MAGIC %pip install mlflow databricks-sql-connector --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SQL-Based Agent Model

# COMMAND ----------

class SQLRiskBricksAgent(mlflow.pyfunc.PythonModel):
    """
    Agent that uses Databricks SQL connector to query UC functions.
    """
    
    def load_context(self, context):
        """Initialize with SQL connection parameters."""
        self.warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID', 'default')
    
    def predict(self, context, model_input):
        """
        Process user queries and return responses.
        """
        from databricks import sql
        import os
        
        # Extract the query
        if isinstance(model_input, pd.DataFrame):
            query = model_input.iloc[0]['input'] if 'input' in model_input.columns else str(model_input.iloc[0, 0])
        else:
            query = str(model_input)
        
        query_lower = query.lower()
        
        # Get connection parameters from environment
        host = os.environ.get('DATABRICKS_HOST', '').replace('https://', '')
        token = os.environ.get('DATABRICKS_TOKEN', '')
        
        if not host or not token:
            return [f"Configuration error: Missing DATABRICKS_HOST or DATABRICKS_TOKEN environment variables"]
        
        try:
            # Create SQL connection
            connection = sql.connect(
                server_hostname=host,
                http_path=f'/sql/1.0/warehouses/{self.warehouse_id}',
                access_token=token
            )
            
            cursor = connection.cursor()
            
            # Route queries to appropriate UC functions
            
            # Portfolio Manager risk profile
            if 'sarah' in query_lower and 'risk' in query_lower:
                cursor.execute("SELECT * FROM riskbricks.agent_tools.get_risk_metrics('Sarah Russel')")
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if result:
                    row = dict(zip(columns, result[0]))
                    response = f"""Sarah Russel is a Conservative portfolio manager with:
- Total AUM: ${row['aum_usd']:,.0f}
- Portfolio Beta: {row['portfolio_beta']:.2f}
- Volatility: {row['weighted_volatility_pct']:.2f}%
- 1-Day VaR (95%): ${row['var_1day_95_usd']:,.0f}
- Number of positions: {row['num_positions']}

Her portfolio focuses on low-risk, stable companies with defensive characteristics."""
                    cursor.close()
                    connection.close()
                    return [response]
            
            # Compare managers
            elif 'compare' in query_lower or 'all managers' in query_lower or 'three' in query_lower:
                cursor.execute("SELECT * FROM riskbricks.agent_tools.compare_managers()")
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if result:
                    comparison = []
                    total_aum = 0
                    for row_tuple in result:
                        row = dict(zip(columns, row_tuple))
                        total_aum += row['aum_usd']
                        comparison.append(f"""**{row['manager_name']}** ({row['risk_profile']}):
- AUM: ${row['aum_usd']:,.0f}
- Beta: {row['portfolio_beta']:.2f}
- VaR (1-day, 95%): ${row['var_1day_95_usd']:,.0f}
- Holdings: {row['num_holdings']}
- Target Return: {row['target_return_pct']:.0f}%
- Max Volatility: {row['max_volatility_pct']:.0f}%""")
                    
                    response = f"""Portfolio Manager Comparison:

{chr(10).join(comparison)}

Total AUM across all managers: ${total_aum:,.0f}"""
                    cursor.close()
                    connection.close()
                    return [response]
            
            # Sector exposure
            elif 'sector' in query_lower and 'mohit' in query_lower:
                cursor.execute("SELECT * FROM riskbricks.agent_tools.get_sector_exposures('Mohit Arora')")
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                if result:
                    sectors = [f"- {dict(zip(columns, row))['sector']}: {dict(zip(columns, row))['sector_weight_pct']:.1f}%" for row in result]
                    response = f"""Mohit Arora's Sector Exposure:

{chr(10).join(sectors)}"""
                    cursor.close()
                    connection.close()
                    return [response]
            
            # Holdings
            elif 'holdings' in query_lower or 'positions' in query_lower:
                manager_name = None
                if 'sarah' in query_lower:
                    manager_name = 'Sarah Russel'
                elif 'rena' in query_lower:
                    manager_name = 'Rena Tang'
                elif 'mohit' in query_lower:
                    manager_name = 'Mohit Arora'
                
                if manager_name:
                    cursor.execute(f"SELECT * FROM riskbricks.agent_tools.get_portfolio_holdings('{manager_name}') LIMIT 10")
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    
                    if result:
                        holdings = [f"- {dict(zip(columns, row))['symbol']}: ${dict(zip(columns, row))['value_usd']:,.0f} ({dict(zip(columns, row))['weight_pct']:.1f}%)" for row in result]
                        response = f"""{manager_name}'s Top 10 Holdings:

{chr(10).join(holdings)}"""
                        cursor.close()
                        connection.close()
                        return [response]
            
            # Default response
            cursor.close()
            connection.close()
            
            return [f"""I can help you with:

1. Portfolio manager risk profiles (e.g., "What is Sarah Russel's risk profile?")
2. Portfolio holdings (e.g., "Show me Mohit Arora's holdings")
3. Sector exposures (e.g., "What is Mohit Arora's sector exposure?")
4. Manager comparisons (e.g., "Compare all three managers")

Your query: "{query}"

Please ask a specific question about Sarah Russel, Rena Tang, or Mohit Arora."""]
        
        except Exception as e:
            return [f"Error processing query: {str(e)}\n\nPlease try asking about Sarah Russel, Rena Tang, or Mohit Arora's portfolios."]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Agent

# COMMAND ----------

# Create a sample input
sample_input = pd.DataFrame({
    "input": ["What is Sarah Russel's risk profile?"]
})

# Create experiment
mlflow.set_experiment("/Shared/RiskBricks/sql_agent")

# Get warehouse ID from Spark
warehouse_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

with mlflow.start_run(run_name="sql_riskbricks_agent") as run:
    
    # Set environment variable for warehouse
    mlflow.log_param("warehouse_id", warehouse_id)
    
    # Log the model
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=SQLRiskBricksAgent(),
        input_example=sample_input,
        signature=mlflow.models.infer_signature(
            sample_input,
            ["Sarah Russel is a Conservative portfolio manager..."]
        ),
        pip_requirements=[
            "mlflow>=2.10.0",
            "databricks-sql-connector>=3.0.0"
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
experiment = client.get_experiment_by_name("/Shared/RiskBricks/sql_agent")
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
        "type": "sql_agent",
        "version": "3.0"
    }
)

print(f"✅ Model registered to Unity Catalog!")
print(f"📦 Model: {model_name}")
print(f"🔢 Version: {model_version.version}")

# COMMAND ----------

print(f"""
🎉 SQL AGENT DEPLOYED!

Next Steps:
1. Update the serving endpoint to use version {model_version.version}
2. Make sure environment variables are set in serving endpoint config:
   - DATABRICKS_HOST
   - DATABRICKS_TOKEN
   - DATABRICKS_WAREHOUSE_ID

Model Details:
- Name: {model_name}
- Version: {model_version.version}
- Run ID: {latest_run.info.run_id}
- Uses: databricks-sql-connector (no PySpark dependency)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

