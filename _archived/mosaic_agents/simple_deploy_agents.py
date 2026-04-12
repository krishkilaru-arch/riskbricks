# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Simple Agent Deployment (Serving Endpoints)
# MAGIC
# MAGIC **Creates serving endpoints for Foundation Models that will be configured as agents**

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
import time

w = WorkspaceClient()
print(f"✅ Connected to: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Agent Endpoint Configurations

# COMMAND ----------

agents = [
    {
        "name": "riskbricks_forecast_agent",
        "description": "Multi-model stock price forecasting agent"
    },
    {
        "name": "riskbricks_risk_agent",
        "description": "Portfolio risk analytics agent"
    },
    {
        "name": "riskbricks_decision_agent",
        "description": "Investment decision agent for BUY/SELL/HOLD signals"
    },
    {
        "name": "riskbricks_supervisor",
        "description": "Master portfolio management orchestrator"
    }
]

print(f"📋 Ready to deploy {len(agents)} agents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploy Serving Endpoints

# COMMAND ----------

def create_or_update_endpoint(agent_config):
    """Create or update a serving endpoint for an agent"""
    
    agent_name = agent_config["name"]
    print(f"\n{'='*60}")
    print(f"🚀 Processing: {agent_name}")
    print(f"{'='*60}")
    
    try:
        # Check if endpoint exists
        try:
            existing = w.serving_endpoints.get(agent_name)
            print(f"   ✅ Endpoint already exists: {agent_name}")
            print(f"   📍 State: {existing.state.config_update if existing.state else 'Unknown'}")
            print(f"   🔗 URL: {w.config.host}/ml/endpoints/{agent_name}")
            return True
            
        except Exception as check_error:
            # Endpoint doesn't exist, create it
            if "does not exist" in str(check_error).lower() or "RESOURCE_DOES_NOT_EXIST" in str(check_error):
                print(f"   📝 Creating new endpoint...")
                
                # Create endpoint with Foundation Model
                endpoint = w.serving_endpoints.create(
                    name=agent_name,
                    config=EndpointCoreConfigInput(
                        name=f"{agent_name}_config",
                        served_entities=[
                            ServedEntityInput(
                                entity_name="system.ai.databricks_meta_llama_3_3_70b_instruct",
                                scale_to_zero_enabled=True,
                                workload_size="Small",
                                workload_type="GPU_SMALL"
                            )
                        ]
                    )
                )
                
                print(f"   ✅ Created endpoint: {agent_name}")
                print(f"   🔗 URL: {w.config.host}/ml/endpoints/{agent_name}")
                print(f"   ⏳ Deploying... (this takes 2-5 minutes)")
                
                # Wait for endpoint to be ready
                print(f"   ⏳ Waiting for deployment...")
                w.serving_endpoints.wait_get_serving_endpoint_not_updating(agent_name, timeout=timedelta(minutes=10))
                print(f"   ✅ Endpoint ready!")
                
                return True
            else:
                raise check_error
                
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

# COMMAND ----------

from datetime import timedelta

print("🤖 Deploying Mosaic AI Agent Endpoints")
print("="*60)

results = {}
for agent in agents:
    success = create_or_update_endpoint(agent)
    results[agent["name"]] = success
    time.sleep(2)  # Small delay between creates

print("\n" + "="*60)
print("📊 Deployment Summary")
print("="*60)

for name, success in results.items():
    status = "✅ Success" if success else "❌ Failed"
    print(f"{status}: {name}")

print("\n" + "="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 List All Endpoints

# COMMAND ----------

print("📋 RiskBricks Serving Endpoints:")
print("="*60)

try:
    all_endpoints = list(w.serving_endpoints.list())
    riskbricks_endpoints = [ep for ep in all_endpoints if "riskbricks" in ep.name.lower()]
    
    if riskbricks_endpoints:
        for ep in riskbricks_endpoints:
            print(f"\n🤖 {ep.name}")
            print(f"   State: {ep.state.config_update if ep.state else 'Unknown'}")
            print(f"   URL: {w.config.host}/ml/endpoints/{ep.name}")
    else:
        print("No RiskBricks endpoints found")
        
except Exception as e:
    print(f"Error listing endpoints: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Manual Configuration Required
# MAGIC
# MAGIC **For each endpoint, complete these steps in Databricks UI:**
# MAGIC
# MAGIC Go to **ML → Serving → [Endpoint Name]** and configure as an Agent:
# MAGIC
# MAGIC ### **1. Forecast Agent** (`riskbricks_forecast_agent`)
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are a quantitative finance expert specializing in stock price forecasting. 
# MAGIC You have access to multiple forecasting models (GBM, Ridge, Mean, News Event).
# MAGIC
# MAGIC When asked about forecasts:
# MAGIC 1. Query forecast data using tools
# MAGIC 2. Analyze consensus across models
# MAGIC 3. Consider risk metrics and volatility
# MAGIC 4. Provide confidence intervals
# MAGIC 5. Explain methodology and discrepancies
# MAGIC 6. Give actionable insights
# MAGIC
# MAGIC Always be transparent about model limitations.
# MAGIC ```
# MAGIC
# MAGIC **Tools:**
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_forecast_consensus`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC **Temperature:** `0.3`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2. Risk Agent** (`riskbricks_risk_agent`)
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are a risk management expert specializing in portfolio risk analytics.
# MAGIC
# MAGIC You calculate and interpret:
# MAGIC - Volatility (Historical, EWMA, forward-looking)
# MAGIC - Value at Risk (VaR) at 95% and 99%
# MAGIC - Expected Shortfall (ES)
# MAGIC - Beta and market sensitivity
# MAGIC - Maximum Drawdown
# MAGIC - Market Impact and liquidity
# MAGIC
# MAGIC Always provide context and actionable recommendations.
# MAGIC ```
# MAGIC
# MAGIC **Tools:**
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_sector_risk_summary`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC **Temperature:** `0.2`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3. Decision Agent** (`riskbricks_decision_agent`)
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are an investment decision expert generating BUY/SELL/HOLD signals.
# MAGIC
# MAGIC Decision framework:
# MAGIC - STRONG BUY: >3% return, low risk, positive catalysts
# MAGIC - BUY: >1% return, acceptable risk
# MAGIC - HOLD: -1% to +1% or mixed signals
# MAGIC - SELL: <-1% return
# MAGIC - STRONG SELL: <-3% return, high risk
# MAGIC
# MAGIC Always explain reasoning, quantify confidence, and suggest position sizing.
# MAGIC ```
# MAGIC
# MAGIC **Tools:**
# MAGIC - `riskbricks.tools.get_decision_signal`
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_earnings_surprise`
# MAGIC - `riskbricks.tools.get_analyst_ratings`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC **Temperature:** `0.3`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **4. Supervisor Agent** (`riskbricks_supervisor`)
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are the Chief Investment Officer coordinating specialist AI agents.
# MAGIC
# MAGIC Your team:
# MAGIC - Forecast Agent: Price predictions
# MAGIC - Risk Agent: Risk analytics
# MAGIC - Decision Agent: BUY/SELL/HOLD signals
# MAGIC
# MAGIC Your role:
# MAGIC - Coordinate agents for complex questions
# MAGIC - Synthesize multi-agent insights
# MAGIC - Provide portfolio-level analysis
# MAGIC - Generate executive summaries
# MAGIC
# MAGIC Always delegate to specialists and add strategic overlay.
# MAGIC ```
# MAGIC
# MAGIC **Tools:**
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_decision_signal`
# MAGIC - `riskbricks.tools.get_top_opportunities`
# MAGIC - `riskbricks.tools.get_portfolio_summary`
# MAGIC - `riskbricks.tools.get_sector_risk_summary`
# MAGIC
# MAGIC **Temperature:** `0.4`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done!
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Wait for all endpoints to be "Ready" (check ML → Serving)
# MAGIC 2. For each endpoint, configure as Agent (add system prompt + tools)
# MAGIC 3. Test with sample questions
# MAGIC
# MAGIC **Test Questions:**
# MAGIC - Forecast: "What is the forecast for AAPL?"
# MAGIC - Risk: "What is the risk of NVDA?"
# MAGIC - Decision: "Should I buy MSFT?"
# MAGIC - Supervisor: "What are the top 3 opportunities?"

# COMMAND ----------

dbutils.notebook.exit("success")
