# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Create Agent Serving Endpoints (REST API)
# MAGIC
# MAGIC **Simple REST API approach to create serving endpoints**

# COMMAND ----------

import requests
import json
import time

# Get authentication
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
api_url = ctx.apiUrl().get()

headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

print(f"✅ API URL: {api_url}")
print(f"✅ Token configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Agent Definitions

# COMMAND ----------

agents = [
    "riskbricks_forecast_agent",
    "riskbricks_risk_agent", 
    "riskbricks_decision_agent",
    "riskbricks_supervisor"
]

print(f"📋 Agents to create: {len(agents)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Create Endpoints

# COMMAND ----------

def create_serving_endpoint(endpoint_name):
    """Create a serving endpoint for Foundation Model"""
    
    print(f"\n{'='*60}")
    print(f"🚀 Creating: {endpoint_name}")
    print(f"{'='*60}")
    
    # Check if exists first
    check_url = f"{api_url}/api/2.0/serving-endpoints/{endpoint_name}"
    check_response = requests.get(check_url, headers=headers)
    
    if check_response.status_code == 200:
        print(f"   ✅ Endpoint already exists: {endpoint_name}")
        print(f"   🔗 URL: {api_url}/ml/endpoints/{endpoint_name}")
        return True
    
    # Create new endpoint
    create_url = f"{api_url}/api/2.0/serving-endpoints"
    
    payload = {
        "name": endpoint_name,
        "config": {
            "served_entities": [
                {
                    "entity_name": "system.ai.llama_v3_3_70b_instruct",
                    "scale_to_zero_enabled": True,
                    "min_provisioned_throughput": 0,
                    "max_provisioned_throughput": 100
                }
            ]
        }
    }
    
    try:
        print(f"   📝 Creating endpoint...")
        response = requests.post(create_url, headers=headers, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"   ✅ Created: {endpoint_name}")
            print(f"   🔗 URL: {api_url}/ml/endpoints/{endpoint_name}")
            print(f"   ⏳ Deploying... (check status in UI)")
            return True
        else:
            print(f"   ❌ Failed: {response.status_code}")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

# COMMAND ----------

print("🤖 Creating Mosaic AI Agent Endpoints")
print("="*60)

results = {}
for agent_name in agents:
    success = create_serving_endpoint(agent_name)
    results[agent_name] = success
    time.sleep(1)

print("\n" + "="*60)
print("📊 Deployment Summary")
print("="*60)

for name, success in results.items():
    status = "✅" if success else "❌"
    print(f"{status} {name}")

print("\n" + "="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 List All Endpoints

# COMMAND ----------

print("📋 Listing All Serving Endpoints:")
print("="*60)

list_url = f"{api_url}/api/2.0/serving-endpoints"
response = requests.get(list_url, headers=headers)

if response.status_code == 200:
    data = response.json()
    endpoints = data.get("endpoints", [])
    
    riskbricks_eps = [ep for ep in endpoints if "riskbricks" in ep.get("name", "").lower()]
    
    if riskbricks_eps:
        for ep in riskbricks_eps:
            print(f"\n🤖 {ep['name']}")
            print(f"   State: {ep.get('state', {}).get('ready', 'Unknown')}")
            print(f"   URL: {api_url}/ml/endpoints/{ep['name']}")
    else:
        print("\nNo RiskBricks endpoints found yet")
else:
    print(f"Error listing endpoints: {response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Next Steps: Configure as Agents
# MAGIC
# MAGIC **For each endpoint, go to ML → Serving → [Endpoint Name] and:**
# MAGIC
# MAGIC 1. Wait for endpoint to be "Ready" (2-5 minutes)
# MAGIC 2. Enable "Agent" mode
# MAGIC 3. Add system prompt (see below)
# MAGIC 4. Add UC function tools (see below)
# MAGIC 5. Test with sample questions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Forecast Agent Configuration**
# MAGIC
# MAGIC **Endpoint:** `riskbricks_forecast_agent`
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are a quantitative finance expert specializing in stock price forecasting.
# MAGIC Use your tools to query forecast data, analyze model consensus, and provide
# MAGIC actionable insights with confidence intervals. Always explain your methodology
# MAGIC and be transparent about model limitations.
# MAGIC ```
# MAGIC
# MAGIC **UC Tools:**
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_forecast_consensus`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC **Temperature:** `0.3`
# MAGIC
# MAGIC **Test:** "What is the forecast for AAPL?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Risk Agent Configuration**
# MAGIC
# MAGIC **Endpoint:** `riskbricks_risk_agent`
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are a risk management expert specializing in portfolio analytics.
# MAGIC Calculate and interpret volatility, VaR, beta, drawdown, and market impact.
# MAGIC Compare metrics across stocks and sectors, and provide actionable
# MAGIC risk management recommendations.
# MAGIC ```
# MAGIC
# MAGIC **UC Tools:**
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_sector_risk_summary`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC **Temperature:** `0.2`
# MAGIC
# MAGIC **Test:** "What is the risk profile of NVDA?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Decision Agent Configuration**
# MAGIC
# MAGIC **Endpoint:** `riskbricks_decision_agent`
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are an investment decision expert generating BUY/SELL/HOLD signals.
# MAGIC
# MAGIC Framework:
# MAGIC - STRONG BUY: >3% return, low risk
# MAGIC - BUY: >1% return, acceptable risk
# MAGIC - HOLD: -1% to +1%
# MAGIC - SELL: <-1% return
# MAGIC - STRONG SELL: <-3% return, high risk
# MAGIC
# MAGIC Always explain reasoning, quantify confidence, and suggest position sizing.
# MAGIC ```
# MAGIC
# MAGIC **UC Tools:**
# MAGIC - `riskbricks.tools.get_decision_signal`
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_earnings_surprise`
# MAGIC - `riskbricks.tools.get_analyst_ratings`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC **Temperature:** `0.3`
# MAGIC
# MAGIC **Test:** "Should I buy MSFT?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Supervisor Agent Configuration**
# MAGIC
# MAGIC **Endpoint:** `riskbricks_supervisor`
# MAGIC
# MAGIC **System Prompt:**
# MAGIC ```
# MAGIC You are the Chief Investment Officer coordinating specialist AI agents
# MAGIC for Forecast, Risk, and Decision analysis. Synthesize multi-agent insights,
# MAGIC provide portfolio-level analysis, and generate executive summaries with
# MAGIC strategic recommendations.
# MAGIC ```
# MAGIC
# MAGIC **UC Tools:**
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_decision_signal`
# MAGIC - `riskbricks.tools.get_top_opportunities`
# MAGIC - `riskbricks.tools.get_portfolio_summary`
# MAGIC - `riskbricks.tools.get_sector_risk_summary`
# MAGIC
# MAGIC **Temperature:** `0.4`
# MAGIC
# MAGIC **Test:** "What are the top 3 investment opportunities?"

# COMMAND ----------

print("✅ Endpoint creation complete!")
print("\n📝 Next: Configure each endpoint as an Agent in the UI")
print("📍 Go to: ML → Serving → [Endpoint Name]")

# COMMAND ----------

dbutils.notebook.exit("success")
