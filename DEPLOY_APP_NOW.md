# 🚀 Deploy Working Agent - Quick Guide

## ✅ Status
- ✅ Streamlit App deployed and working
- ✅ Authentication working
- ⚠️ Agent returning test response (needs real agent registration)

## 🔧 Quick Fix - 3 Steps

### Step 1: Run Simple Agent Notebook
1. Open Databricks workspace
2. Navigate to: `/Shared/RiskBricks/notebooks/04_agents/05_simple_agent.py`
3. **Run all cells** (click "Run All" button)
4. Wait ~2 minutes for completion
5. Note the **version number** at the end (will be "2" or higher)

### Step 2: Update Serving Endpoint

Run this in a Databricks notebook cell:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Update to use the new agent version
w.serving_endpoints.update_config(
    name="riskbricks-agent-endpoint",
    served_entities=[{
        "entity_name": "riskbricks.agents.riskbricks_supervisor",
        "entity_version": "2",  # Use the version from Step 1
        "workload_size": "Small",
        "scale_to_zero_enabled": True
    }]
)

print("✅ Serving endpoint updated!")
```

### Step 3: Test the App

1. Open: https://riskbricks-app-7474660441663212.aws.databricksapps.com
2. Go to: 🤖 AI Agent Chat
3. Try these queries:
   - "What is Sarah Russel's risk profile?"
   - "Compare all three managers"
   - "Show me Mohit Arora's holdings"
   - "What is Rena Tang's sector exposure?"

## 🎯 What the Simple Agent Can Do

The simple agent can answer:

1. **Risk Profiles**: "What is [manager name]'s risk profile?"
2. **Holdings**: "Show me [manager name]'s holdings"
3. **Sector Exposure**: "What is [manager name]'s sector exposure?"
4. **Stress Tests**: "Show stress tests for [manager name]"
5. **Comparisons**: "Compare all three managers"
6. **Macro Context**: "What's the current macro environment?"

### Manager Names:
- Sarah Russel (Conservative)
- Rena Tang (Balanced)
- Mohit Arora (Aggressive)

## 🔍 How It Works

The simple agent:
- Uses Unity Catalog functions directly (no LangChain complexity)
- Pattern matches queries to appropriate UC functions
- Returns formatted results from Gold layer tables
- Fast and reliable for demo purposes

## 🚀 Next Steps (After Testing)

Once the simple agent works:
1. ✅ Test all Streamlit pages
2. ✅ Add the 6 new UC functions (stock analysis, correlations, etc.)
3. ✅ Upgrade to full LangChain agent with dynamic tool selection
4. ✅ Add Agent Bricks Multi-Agent Supervisor

## 📊 Current Architecture

```
Streamlit App (authenticated with SDK)
    ↓
Serving Endpoint: riskbricks-agent-endpoint
    ↓
Model: riskbricks.agents.riskbricks_supervisor (v2)
    ↓
Unity Catalog Functions (12 functions)
    ↓
Gold Layer Tables
```

## ❓ Troubleshooting

**Still getting test response?**
- Wait 1-2 minutes after updating endpoint (cold start)
- Check endpoint status: `databricks serving-endpoints get riskbricks-agent-endpoint`
- Verify version is updated in the output

**Error querying agent?**
- Check app logs in Databricks Apps UI
- Verify UC functions exist: Run `SHOW FUNCTIONS IN riskbricks.agent_tools`

**Need help?**
- Check serving endpoint logs in Databricks UI
- View model details: `databricks registered-models get riskbricks.agents.riskbricks_supervisor`
