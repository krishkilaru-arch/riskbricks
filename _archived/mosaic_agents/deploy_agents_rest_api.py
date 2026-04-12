# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Deploy Mosaic AI Agents via REST API
# MAGIC
# MAGIC **Alternative deployment method using Databricks REST API**
# MAGIC
# MAGIC This notebook uses the REST API to create GenAI agents with tools.

# COMMAND ----------

import requests
import json

# Get auth info
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
api_url = ctx.apiUrl().get()

print(f"✅ API URL: {api_url}")
print(f"✅ Token: {'*' * 20}{api_token[-4:]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Agent Configurations

# COMMAND ----------

agents_config = {
    "forecast": {
        "name": "riskbricks_forecast_agent",
        "description": "Multi-model stock price forecasting agent",
        "instructions": """You are a quantitative finance expert specializing in stock price forecasting. You have access to multiple forecasting models (Geometric Brownian Motion, Ridge Regression, Simple Mean, and News Event-based models). When asked about a stock forecast:

1. Query the forecast data using available tools
2. Analyze consensus across models
3. Consider risk metrics and volatility
4. Provide confidence intervals
5. Explain the forecast methodology
6. Highlight any discrepancies between models
7. Give actionable insights

Always be transparent about model limitations and uncertainty. Reference specific model outputs when explaining predictions.""",
        "model": "databricks-meta-llama-3-3-70b-instruct",
        "tools": [
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_latest_forecast"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_forecast_consensus"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_risk_metrics"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_company_info"}
        ]
    },
    "risk": {
        "name": "riskbricks_risk_agent",
        "description": "Portfolio risk analytics agent",
        "instructions": """You are a risk management expert specializing in portfolio risk analytics. You calculate and interpret volatility, VaR, beta, drawdown, and market impact.

When analyzing risk:
- Compare metrics across stocks and sectors
- Identify concentration risks
- Suggest diversification opportunities
- Explain risk-adjusted returns
- Highlight tail risks and stress scenarios

Always provide context and actionable risk management recommendations.""",
        "model": "databricks-meta-llama-3-3-70b-instruct",
        "tools": [
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_risk_metrics"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_sector_risk_summary"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_company_info"}
        ]
    },
    "decision": {
        "name": "riskbricks_decision_agent",
        "description": "Investment decision agent for BUY/SELL/HOLD signals",
        "instructions": """You are an investment decision-making expert who synthesizes forecasts, risk metrics, and alternative signals to generate actionable trading signals.

Your decision framework:
- STRONG BUY: High expected return (>3%), low risk, positive catalysts
- BUY: Positive expected return (>1%), acceptable risk
- HOLD: Near-zero return (-1% to +1%) or mixed signals
- SELL: Negative expected return (<-1%)
- STRONG SELL: Large expected loss (<-3%), high risk

Always explain your reasoning, quantify confidence, and suggest position sizing.""",
        "model": "databricks-meta-llama-3-3-70b-instruct",
        "tools": [
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_decision_signal"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_latest_forecast"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_risk_metrics"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_earnings_surprise"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_analyst_ratings"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_company_info"}
        ]
    },
    "supervisor": {
        "name": "riskbricks_supervisor",
        "description": "Master portfolio management agent that coordinates all specialists",
        "instructions": """You are the Chief Investment Officer overseeing specialized AI agents for Forecast, Risk, and Decision analysis.

Your Role:
- Coordinate agents to answer complex investment questions
- Synthesize multi-agent insights into cohesive recommendations
- Provide portfolio-level analysis
- Consider macro factors, sector rotations, and correlations
- Generate executive summaries

Always delegate to specialists, aggregate outputs, and add strategic overlay.""",
        "model": "databricks-meta-llama-3-3-70b-instruct",
        "tools": [
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_latest_forecast"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_risk_metrics"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_decision_signal"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_top_opportunities"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_portfolio_summary"},
            {"type": "uc_function", "uc_function_name": "riskbricks.tools.get_sector_risk_summary"}
        ]
    }
}

print("✅ Agent configurations loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Deploy Function

# COMMAND ----------

def deploy_agent_via_api(agent_key, agent_config):
    """Deploy agent using Databricks REST API"""
    
    agent_name = agent_config["name"]
    print(f"\n🚀 Deploying: {agent_name}")
    print("=" * 60)
    
    # Prepare payload
    payload = {
        "name": agent_name,
        "description": agent_config["description"],
        "model": agent_config["model"],
        "instructions": agent_config["instructions"],
        "tools": agent_config["tools"]
    }
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    # Try to create agent
    try:
        # Check if agent exists
        list_url = f"{api_url}/api/2.0/serving-endpoints"
        list_response = requests.get(list_url, headers=headers)
        
        if list_response.status_code == 200:
            existing_agents = list_response.json().get("endpoints", [])
            agent_exists = any(ep["name"] == agent_name for ep in existing_agents)
            
            if agent_exists:
                print(f"   ⚠️  Agent '{agent_name}' already exists")
                print(f"   💡 To update, delete it first via UI or API")
                return True
        
        # Create new agent endpoint
        create_url = f"{api_url}/api/2.0/serving-endpoints"
        
        endpoint_config = {
            "name": agent_name,
            "config": {
                "served_entities": [{
                    "name": f"{agent_name}_v1",
                    "external_model": {
                        "name": agent_config["model"],
                        "provider": "databricks-model-serving",
                        "task": "llm/v1/chat"
                    }
                }],
                "traffic_config": {
                    "routes": [{
                        "served_model_name": f"{agent_name}_v1",
                        "traffic_percentage": 100
                    }]
                }
            }
        }
        
        create_response = requests.post(create_url, headers=headers, json=endpoint_config)
        
        if create_response.status_code in [200, 201]:
            print(f"   ✅ Endpoint created: {agent_name}")
            print(f"   🔗 URL: {api_url}/ml/endpoints/{agent_name}")
            
            # Note: Tools and system prompts need separate configuration
            print(f"\n   ℹ️  Next Steps:")
            print(f"   1. Go to ML → Agents → {agent_name}")
            print(f"   2. Configure system prompt")
            print(f"   3. Add UC function tools")
            
            return True
        else:
            print(f"   ❌ Failed to create endpoint: {create_response.status_code}")
            print(f"   Response: {create_response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Deploy All Agents

# COMMAND ----------

print("🤖 Deploying All Mosaic AI Agents")
print("=" * 60)

results = {}
for agent_key, agent_config in agents_config.items():
    success = deploy_agent_via_api(agent_key, agent_config)
    results[agent_config["name"]] = success

print("\n" + "=" * 60)
print("📊 Deployment Summary")
print("=" * 60)

for name, success in results.items():
    status = "✅" if success else "❌"
    print(f"{status} {name}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Manual Configuration Guide
# MAGIC
# MAGIC **For each agent, complete these steps in Databricks UI:**
# MAGIC
# MAGIC ### **1. Forecast Agent** (`riskbricks_forecast_agent`)
# MAGIC **Tools to add:**
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_forecast_consensus`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC ### **2. Risk Agent** (`riskbricks_risk_agent`)
# MAGIC **Tools to add:**
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_sector_risk_summary`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC ### **3. Decision Agent** (`riskbricks_decision_agent`)
# MAGIC **Tools to add:**
# MAGIC - `riskbricks.tools.get_decision_signal`
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_earnings_surprise`
# MAGIC - `riskbricks.tools.get_analyst_ratings`
# MAGIC - `riskbricks.tools.get_company_info`
# MAGIC
# MAGIC ### **4. Supervisor Agent** (`riskbricks_supervisor`)
# MAGIC **Tools to add:**
# MAGIC - `riskbricks.tools.get_latest_forecast`
# MAGIC - `riskbricks.tools.get_risk_metrics`
# MAGIC - `riskbricks.tools.get_decision_signal`
# MAGIC - `riskbricks.tools.get_top_opportunities`
# MAGIC - `riskbricks.tools.get_portfolio_summary`
# MAGIC - `riskbricks.tools.get_sector_risk_summary`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Test Query Function

# COMMAND ----------

def test_agent(agent_name, question):
    """Test an agent with a question"""
    
    print(f"\n🧪 Testing: {agent_name}")
    print(f"❓ Question: {question}")
    print("=" * 60)
    
    url = f"{api_url}/serving-endpoints/{agent_name}/invocations"
    
    payload = {
        "messages": [
            {"role": "user", "content": question}
        ]
    }
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Response:")
            print(json.dumps(result, indent=2))
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")

# Uncomment to test after configuration:
# test_agent("riskbricks_forecast_agent", "What is the forecast for AAPL?")

# COMMAND ----------

dbutils.notebook.exit("success")
