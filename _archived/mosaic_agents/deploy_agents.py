# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Deploy Mosaic AI Agents Programmatically
# MAGIC
# MAGIC **Purpose**: Deploy all 4 RiskBricks Mosaic AI agents using Python
# MAGIC
# MAGIC **Agents to Deploy:**
# MAGIC 1. Forecast Agent - Price predictions
# MAGIC 2. Risk Agent - Risk analytics
# MAGIC 3. Decision Agent - BUY/SELL/HOLD signals
# MAGIC 4. Supervisor Agent - Portfolio management

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Databricks SDK

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving
import json

# Initialize Databricks client
w = WorkspaceClient()

print("✅ Databricks SDK initialized")
print(f"✅ Workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Define Agent Configurations

# COMMAND ----------

# Agent 1: Forecast Agent
forecast_agent_config = {
    "name": "riskbricks_forecast_agent",
    "description": "Multi-model stock price forecasting agent that combines GBM, Ridge, Mean, and News Event models",
    "model_name": "databricks-meta-llama-3-3-70b-instruct",
    "system_message": """You are a quantitative finance expert specializing in stock price forecasting. You have access to multiple forecasting models (Geometric Brownian Motion, Ridge Regression, Simple Mean, and News Event-based models). When asked about a stock forecast:

1. Query the forecast data using available tools
2. Analyze consensus across models
3. Consider risk metrics and volatility
4. Provide confidence intervals
5. Explain the forecast methodology
6. Highlight any discrepancies between models
7. Give actionable insights

Always be transparent about model limitations and uncertainty. Reference specific model outputs when explaining predictions.""",
    "temperature": 0.3,
    "max_tokens": 2000,
    "tools": [
        {"name": "riskbricks.tools.get_latest_forecast", "type": "uc_function"},
        {"name": "riskbricks.tools.get_forecast_consensus", "type": "uc_function"},
        {"name": "riskbricks.tools.get_risk_metrics", "type": "uc_function"},
        {"name": "riskbricks.tools.get_company_info", "type": "uc_function"}
    ]
}

print("✅ Forecast Agent config defined")

# COMMAND ----------

# Agent 2: Risk Agent
risk_agent_config = {
    "name": "riskbricks_risk_agent",
    "description": "Portfolio risk analytics agent that calculates volatility, VaR, beta, maximum drawdown, and market impact",
    "model_name": "databricks-meta-llama-3-3-70b-instruct",
    "system_message": """You are a risk management expert specializing in portfolio risk analytics. You calculate and interpret:

1. Volatility Metrics: Historical, EWMA, and forward-looking volatility
2. Value at Risk (VaR): 95th and 99th percentile losses
3. Expected Shortfall (ES): Conditional VaR
4. Beta: Market sensitivity and correlation
5. Maximum Drawdown: Peak-to-trough decline
6. Market Impact: Liquidity and trading costs

When analyzing risk:
- Compare metrics across stocks and sectors
- Identify concentration risks
- Suggest diversification opportunities
- Explain risk-adjusted returns
- Highlight tail risks and stress scenarios
- Consider correlations and portfolio effects

Always provide context and actionable risk management recommendations.""",
    "temperature": 0.2,
    "max_tokens": 2000,
    "tools": [
        {"name": "riskbricks.tools.get_risk_metrics", "type": "uc_function"},
        {"name": "riskbricks.tools.get_sector_risk_summary", "type": "uc_function"},
        {"name": "riskbricks.tools.get_company_info", "type": "uc_function"}
    ]
}

print("✅ Risk Agent config defined")

# COMMAND ----------

# Agent 3: Decision Agent
decision_agent_config = {
    "name": "riskbricks_decision_agent",
    "description": "Investment decision agent that combines forecasts, risk metrics, and alternative signals for BUY/SELL/HOLD recommendations",
    "model_name": "databricks-meta-llama-3-3-70b-instruct",
    "system_message": """You are an investment decision-making expert who synthesizes multiple data sources to generate actionable trading signals. You analyze:

1. Forecast Models: Expected returns and model consensus
2. Risk Metrics: Volatility, beta, VaR, maximum drawdown
3. News Sentiment: Recent news impact and sentiment scores
4. Alternative Signals: Earnings surprises, analyst ratings

Your decision framework:
- STRONG BUY: High expected return (>3%), low risk, positive catalysts
- BUY: Positive expected return (>1%), acceptable risk
- HOLD: Near-zero return (-1% to +1%) or mixed signals
- SELL: Negative expected return (<-1%)
- STRONG SELL: Large expected loss (<-3%), high risk, negative catalysts

Always explain your reasoning, quantify confidence, highlight key factors, and suggest position sizing based on risk tolerance.""",
    "temperature": 0.3,
    "max_tokens": 2500,
    "tools": [
        {"name": "riskbricks.tools.get_decision_signal", "type": "uc_function"},
        {"name": "riskbricks.tools.get_latest_forecast", "type": "uc_function"},
        {"name": "riskbricks.tools.get_risk_metrics", "type": "uc_function"},
        {"name": "riskbricks.tools.get_earnings_surprise", "type": "uc_function"},
        {"name": "riskbricks.tools.get_analyst_ratings", "type": "uc_function"},
        {"name": "riskbricks.tools.get_company_info", "type": "uc_function"}
    ]
}

print("✅ Decision Agent config defined")

# COMMAND ----------

# Agent 4: Supervisor Agent
supervisor_agent_config = {
    "name": "riskbricks_supervisor",
    "description": "Master orchestration agent that coordinates all specialist agents for comprehensive portfolio management",
    "model_name": "databricks-meta-llama-3-3-70b-instruct",
    "system_message": """You are the Chief Investment Officer overseeing a team of specialized AI agents:

Your Team:
1. Forecast Agent: Predicts future stock prices using multiple models
2. Risk Agent: Calculates volatility, VaR, beta, and risk metrics
3. Decision Agent: Generates BUY/SELL/HOLD signals

Your Role:
- Coordinate agents to answer complex investment questions
- Synthesize multi-agent insights into cohesive recommendations
- Identify conflicts between agents and resolve them
- Provide portfolio-level analysis (not just individual stocks)
- Consider macro factors, sector rotations, and correlations
- Generate executive summaries for stakeholders

Always:
- Delegate to specialist agents
- Aggregate their outputs
- Add strategic overlay
- Explain trade-offs
- Suggest position sizing
- Highlight portfolio impact""",
    "temperature": 0.4,
    "max_tokens": 3000,
    "tools": [
        {"name": "riskbricks.tools.get_latest_forecast", "type": "uc_function"},
        {"name": "riskbricks.tools.get_risk_metrics", "type": "uc_function"},
        {"name": "riskbricks.tools.get_decision_signal", "type": "uc_function"},
        {"name": "riskbricks.tools.get_top_opportunities", "type": "uc_function"},
        {"name": "riskbricks.tools.get_portfolio_summary", "type": "uc_function"},
        {"name": "riskbricks.tools.get_sector_risk_summary", "type": "uc_function"}
    ]
}

print("✅ Supervisor Agent config defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Deploy Agents Function

# COMMAND ----------

def deploy_agent(config):
    """Deploy a Mosaic AI agent"""
    agent_name = config["name"]
    
    print(f"\n🚀 Deploying: {agent_name}")
    print("=" * 60)
    
    try:
        # Check if agent already exists
        try:
            existing = w.serving_endpoints.get(agent_name)
            print(f"   ⚠️  Agent '{agent_name}' already exists")
            print(f"   🔄 Updating existing agent...")
            
            # Update the agent
            w.serving_endpoints.update_config(
                name=agent_name,
                served_entities=[
                    serving.ServedEntityInput(
                        entity_name=config["model_name"],
                        scale_to_zero_enabled=True,
                        workload_size="Small"
                    )
                ]
            )
            print(f"   ✅ Updated: {agent_name}")
            
        except Exception as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                # Create new agent
                print(f"   📝 Creating new agent...")
                
                endpoint = w.serving_endpoints.create(
                    name=agent_name,
                    config=serving.EndpointCoreConfigInput(
                        served_entities=[
                            serving.ServedEntityInput(
                                entity_name=config["model_name"],
                                scale_to_zero_enabled=True,
                                workload_size="Small"
                            )
                        ]
                    )
                )
                
                print(f"   ✅ Created: {agent_name}")
                print(f"   🔗 Endpoint: {endpoint.name}")
            else:
                raise e
        
        # Note: Agent system prompt and tools are configured separately via Databricks UI
        # The SDK currently doesn't support full agent configuration via API
        print(f"\n   ℹ️  Manual Setup Required:")
        print(f"   1. Go to ML → Serving → {agent_name}")
        print(f"   2. Configure system prompt and tools")
        print(f"   3. Tools to add: {', '.join([t['name'].split('.')[-1] for t in config['tools']])}")
        
    except Exception as e:
        print(f"   ❌ Error deploying {agent_name}: {str(e)}")
        return False
    
    return True

print("✅ Deploy function defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Deploy All Agents

# COMMAND ----------

agents = [
    forecast_agent_config,
    risk_agent_config,
    decision_agent_config,
    supervisor_agent_config
]

print("🤖 Deploying Mosaic AI Agents")
print("=" * 60)

results = []
for agent_config in agents:
    success = deploy_agent(agent_config)
    results.append((agent_config["name"], success))

print("\n" + "=" * 60)
print("📊 Deployment Summary")
print("=" * 60)

for name, success in results:
    status = "✅ Success" if success else "❌ Failed"
    print(f"{status}: {name}")

print("\n" + "=" * 60)
print("✅ Deployment Complete!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ List All Agents

# COMMAND ----------

print("📋 Listing all RiskBricks agents:")
print("=" * 60)

try:
    endpoints = w.serving_endpoints.list()
    
    riskbricks_agents = [ep for ep in endpoints if ep.name.startswith("riskbricks_")]
    
    if riskbricks_agents:
        for agent in riskbricks_agents:
            print(f"\n🤖 {agent.name}")
            print(f"   State: {agent.state.config_update}")
            print(f"   URL: {w.config.host}/ml/endpoints/{agent.name}")
    else:
        print("No RiskBricks agents found")
        
except Exception as e:
    print(f"Error listing agents: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Test Agent (Optional)

# COMMAND ----------

def test_agent(agent_name, test_message):
    """Test an agent with a sample message"""
    print(f"\n🧪 Testing: {agent_name}")
    print(f"📝 Message: {test_message}")
    print("=" * 60)
    
    try:
        # Note: This requires the agent to be fully configured and deployed
        response = w.serving_endpoints.query(
            name=agent_name,
            inputs=[{"query": test_message}]
        )
        
        print(f"✅ Response:")
        print(json.dumps(response, indent=2))
        
    except Exception as e:
        print(f"⚠️  Cannot test yet: {e}")
        print(f"ℹ️  Complete manual configuration first via UI")

# Uncomment to test after manual configuration:
# test_agent("riskbricks_forecast_agent", "What is the forecast for AAPL?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Next Steps
# MAGIC
# MAGIC **Manual Configuration Required:**
# MAGIC
# MAGIC For each agent, go to **ML → Serving → [Agent Name]** and configure:
# MAGIC
# MAGIC ### **Forecast Agent:**
# MAGIC - System Prompt: (from cell above)
# MAGIC - Tools:
# MAGIC   - riskbricks.tools.get_latest_forecast
# MAGIC   - riskbricks.tools.get_forecast_consensus
# MAGIC   - riskbricks.tools.get_risk_metrics
# MAGIC   - riskbricks.tools.get_company_info
# MAGIC
# MAGIC ### **Risk Agent:**
# MAGIC - System Prompt: (from cell above)
# MAGIC - Tools:
# MAGIC   - riskbricks.tools.get_risk_metrics
# MAGIC   - riskbricks.tools.get_sector_risk_summary
# MAGIC   - riskbricks.tools.get_company_info
# MAGIC
# MAGIC ### **Decision Agent:**
# MAGIC - System Prompt: (from cell above)
# MAGIC - Tools:
# MAGIC   - riskbricks.tools.get_decision_signal
# MAGIC   - riskbricks.tools.get_latest_forecast
# MAGIC   - riskbricks.tools.get_risk_metrics
# MAGIC   - riskbricks.tools.get_earnings_surprise
# MAGIC   - riskbricks.tools.get_analyst_ratings
# MAGIC   - riskbricks.tools.get_company_info
# MAGIC
# MAGIC ### **Supervisor Agent:**
# MAGIC - System Prompt: (from cell above)
# MAGIC - Tools:
# MAGIC   - riskbricks.tools.get_latest_forecast
# MAGIC   - riskbricks.tools.get_risk_metrics
# MAGIC   - riskbricks.tools.get_decision_signal
# MAGIC   - riskbricks.tools.get_top_opportunities
# MAGIC   - riskbricks.tools.get_portfolio_summary
# MAGIC   - riskbricks.tools.get_sector_risk_summary

# COMMAND ----------

dbutils.notebook.exit("success")
