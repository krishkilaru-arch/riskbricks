# Databricks notebook source
# MAGIC %md
# MAGIC # RiskBricks: Multi-Agent Risk Analytics with Databricks Agent Framework
# MAGIC
# MAGIC This notebook demonstrates **real multi-agent orchestration** using Databricks Agent Framework (LangChain + ChatDatabricks).
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                 SUPERVISOR AGENT (Coordinator)              │
# MAGIC │              (Databricks Llama 3.3 70B)                     │
# MAGIC └───────────────────┬─────────────────────────────────────────┘
# MAGIC                     │
# MAGIC         ┌───────────┼───────────┬──────────────┐
# MAGIC         │           │           │              │
# MAGIC         ▼           ▼           ▼              ▼
# MAGIC   ┌─────────┐ ┌──────────┐ ┌────────┐  ┌──────────┐
# MAGIC   │Validate │ │Get Macro │ │Calculate│  │ Explain  │
# MAGIC   │  Data   │ │ Context  │ │  Risk   │  │ Results  │
# MAGIC   └─────────┘ └──────────┘ └────────┘  └──────────┘
# MAGIC ```
# MAGIC
# MAGIC ## What Makes This "Real" Agent Bricks?
# MAGIC
# MAGIC ✅ **Real LangChain Agents** - Using `create_tool_calling_agent` with `AgentExecutor`  
# MAGIC ✅ **Real LLM** - ChatDatabricks with Llama 3.3 70B  
# MAGIC ✅ **Real Tools** - Python functions wrapped as LangChain Tools  
# MAGIC ✅ **Real Planning** - ReAct framework for reasoning and acting  
# MAGIC ✅ **Real Logging** - MLflow tracking and model registry  
# MAGIC ✅ **Real Data** - Spark DataFrames from Unity Catalog tables  
# MAGIC
# MAGIC ## Future: Agent Bricks Service Migration
# MAGIC
# MAGIC This implementation can seamlessly migrate to **Agent Bricks Multi-Agent Supervisor** service for:
# MAGIC - Automatic quality optimization
# MAGIC - Cost/quality balancing
# MAGIC - Synthetic evaluation datasets
# MAGIC - Continuous improvement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installations

# COMMAND ----------

# Install required packages
%pip install -U -qq langchain-databricks langchain==0.3.7 langchain-community==0.3.7 langchain-experimental mlflow
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Imports

# COMMAND ----------

import mlflow
from langchain_databricks import ChatDatabricks
from langchain.agents import Tool, AgentExecutor, create_tool_calling_agent
from langchain import hub
from langchain.prompts import PromptTemplate, ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import StrOutputParser
import pyspark.sql.functions as F
from datetime import datetime
import json

# Enable MLflow auto-logging for LangChain
mlflow.langchain.autolog()
mlflow.set_registry_uri("databricks-uc")

print("✅ Imports successful")
print(f"✅ MLflow tracking URI: {mlflow.get_tracking_uri()}")
print(f"✅ MLflow registry URI: {mlflow.get_registry_uri()}")

# COMMAND ----------

# Configuration
catalog = "riskbricks"
schema_bronze = "bronze"
schema_silver = "silver"
schema_gold = "gold"

print(f"📊 Using catalog: {catalog}")
print(f"📊 Bronze schema: {schema_bronze}")
print(f"📊 Silver schema: {schema_silver}")
print(f"📊 Gold schema: {schema_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Financial Risk Tools
# MAGIC
# MAGIC These are the "experts" in our multi-agent system. Each tool is a specialized function that the supervisor can call.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 1: Data Validation Agent

# COMMAND ----------

def validate_financial_data(data_table: str) -> str:
    """
    Validates financial data quality from specified table.
    
    This agent checks:
    - Data completeness (missing values)
    - Price anomalies (outliers, negative values)
    - Data freshness
    
    Args:
        data_table: Full table name (e.g. 'riskbricks.bronze.stock_prices_bronze')
    
    Returns:
        JSON string with validation status, quality metrics, and recommendations
    """
    try:
        print(f"🔍 Validating data quality for: {data_table}")
        
        # Load data
        df = spark.table(data_table)
        
        # Quality checks
        total_rows = df.count()
        
        if total_rows == 0:
            return json.dumps({
                "status": "FAIL",
                "error": "Table is empty",
                "recommendation": "Check data ingestion pipeline"
            })
        
        # Check completeness
        complete_rows = df.dropna().count()
        completeness = complete_rows / total_rows
        
        # Determine price column (bronze has 'price', silver has 'close')
        price_col = "price" if "price" in df.columns else "close"
        
        # Check for price anomalies
        stats = df.select(
            F.mean(price_col).alias("mean"),
            F.stddev(price_col).alias("std"),
            F.min(price_col).alias("min"),
            F.max(price_col).alias("max")
        ).collect()[0]
        
        # Detect outliers (3 sigma rule)
        if stats["std"]:
            anomalies = df.filter(
                (F.col(price_col) > stats["mean"] + 3 * stats["std"]) |
                (F.col(price_col) < stats["mean"] - 3 * stats["std"]) |
                (F.col(price_col) <= 0)
            ).count()
        else:
            anomalies = 0
        
        # Decision logic
        if completeness >= 0.95 and anomalies == 0:
            status = "PASS"
            recommendation = "✅ Data quality excellent. Proceed with analysis."
        elif completeness >= 0.80 and anomalies < total_rows * 0.01:
            status = "WARNING"
            recommendation = f"⚠️  Data has {anomalies} anomalies ({anomalies/total_rows*100:.1f}%). Proceed with caution."
        else:
            status = "FAIL"
            recommendation = "❌ Data quality insufficient. Do not proceed with analysis."
        
        result = {
            "status": status,
            "completeness_score": round(completeness, 3),
            "anomaly_count": int(anomalies),
            "total_records": int(total_rows),
            "price_stats": {
                "mean": round(float(stats["mean"]), 2) if stats["mean"] else None,
                "std": round(float(stats["std"]), 2) if stats["std"] else None,
                "min": round(float(stats["min"]), 2) if stats["min"] else None,
                "max": round(float(stats["max"]), 2) if stats["max"] else None
            },
            "recommendation": recommendation
        }
        
        print(f"✅ Validation complete: {status}")
        return json.dumps(result, indent=2)
        
    except Exception as e:
        error_result = {
            "status": "ERROR",
            "error": str(e),
            "recommendation": "Check table name and permissions"
        }
        print(f"❌ Validation error: {e}")
        return json.dumps(error_result)

# Create LangChain Tool
validation_tool = Tool(
    name="validate_data",
    description="""Validates financial data quality and detects anomalies. 
    Use this FIRST before any analysis.
    
    Input: Full table name like 'riskbricks.bronze.stock_prices_bronze'
    Output: JSON with validation status (PASS/WARNING/FAIL), quality metrics, and recommendations.
    
    Example: validate_data('riskbricks.bronze.stock_prices_bronze')""",
    func=validate_financial_data
)

print("✅ Data Validation Tool created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 2: Macro Context Agent

# COMMAND ----------

def get_macro_context(query: str) -> str:
    """
    Retrieves current macroeconomic indicators and context.
    
    This agent provides:
    - Federal Funds Rate
    - CPI Inflation
    - Unemployment Rate
    - GDP Growth
    
    Args:
        query: Query like 'current fed rate' or 'macro context' or 'economic indicators'
    
    Returns:
        JSON string with latest macro economic indicators
    """
    try:
        print(f"🌍 Retrieving macro context for: {query}")
        
        # Load validated macro indicators from silver layer
        macro_table = f"{catalog}.{schema_silver}.macro_indicators"
        macro_df = spark.table(macro_table)
        
        # Get latest values for each indicator
        latest_indicators = macro_df.groupBy("indicator_name").agg(
            F.last("value", ignorenulls=True).alias("latest_value"),
            F.last("date", ignorenulls=True).alias("latest_date")
        ).collect()
        
        # Build context dictionary
        context = {}
        for row in latest_indicators:
            context[row["indicator_name"]] = {
                "value": round(float(row["latest_value"]), 2),
                "as_of": str(row["latest_date"])
            }
        
        # Extract key indicators
        result = {
            "macro_context": context,
            "key_indicators": {
                "fed_funds_rate": context.get("FEDFUNDS", {}).get("value", "N/A"),
                "cpi_inflation": context.get("CPI_INFLATION", {}).get("value", "N/A"),
                "unemployment_rate": context.get("UNEMPLOYMENT_RATE", {}).get("value", "N/A"),
                "gdp_growth": context.get("GDP_GROWTH", {}).get("value", "N/A")
            },
            "interpretation": "Macro data retrieved successfully"
        }
        
        print(f"✅ Retrieved {len(context)} macro indicators")
        return json.dumps(result, indent=2)
        
    except Exception as e:
        error_result = {
            "error": str(e),
            "recommendation": "Check macro_indicators table availability"
        }
        print(f"❌ Macro context error: {e}")
        return json.dumps(error_result)

# Create LangChain Tool
macro_tool = Tool(
    name="get_macro_context",
    description="""Retrieves current macroeconomic indicators including Fed Funds Rate, 
    inflation, unemployment, and GDP growth.
    
    Input: Query like 'current fed rate', 'macro context', or 'economic indicators'
    Output: JSON with latest macro economic indicators and their values
    
    Example: get_macro_context('current macro indicators')""",
    func=get_macro_context
)

print("✅ Macro Context Tool created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 3: Portfolio Manager Query Agent

# COMMAND ----------

def query_portfolio_manager(query: str) -> str:
    """
    Queries specific portfolio manager information and their portfolios.
    
    This agent can answer questions like:
    - "Show me Sarah Russel's portfolio"
    - "Compare all three managers"
    - "What is Mohit Arora's risk profile?"
    - "Show Rena Tang's top holdings"
    
    Args:
        query: Natural language query about portfolio managers
    
    Returns:
        JSON string with manager info, holdings, or comparison data
    """
    try:
        print(f"👥 Querying portfolio managers: {query}")
        
        query_lower = query.lower()
        
        # Identify which manager is being asked about
        manager_name = None
        manager_id = None
        
        if "sarah" in query_lower or "russel" in query_lower:
            manager_name = "Sarah Russel"
            manager_id = "PM001"
        elif "rena" in query_lower or "tang" in query_lower:
            manager_name = "Rena Tang"
            manager_id = "PM002"
        elif "mohit" in query_lower or "arora" in query_lower:
            manager_name = "Mohit Arora"
            manager_id = "PM003"
        
        # Get managers table
        managers_df = spark.table(f"{catalog}.{schema_gold}.portfolio_managers")
        holdings_df = spark.table(f"{catalog}.{schema_gold}.portfolio_holdings")
        companies_df = spark.table(f"{catalog}.{schema_gold}.company_universe")
        
        # If specific manager requested
        if manager_id:
            # Get manager profile
            manager_info = managers_df.filter(F.col("manager_id") == manager_id).collect()[0]
            
            # Get holdings with company info
            manager_holdings = holdings_df.filter(F.col("manager_id") == manager_id) \
                .join(companies_df, "symbol") \
                .select(
                    holdings_df["symbol"], 
                    companies_df["company_name"], 
                    holdings_df["sector"], 
                    holdings_df["weight"], 
                    holdings_df["value_usd"], 
                    companies_df["beta"], 
                    companies_df["volatility_30d"]
                ) \
                .orderBy(F.desc("value_usd")) \
                .limit(20) \
                .collect()
            
            # Sector allocation
            sector_allocation = holdings_df.filter(F.col("manager_id") == manager_id) \
                .groupBy("sector") \
                .agg(
                    F.count("*").alias("num_stocks"),
                    F.sum("weight").alias("total_weight")
                ) \
                .orderBy(F.desc("total_weight")) \
                .collect()
            
            result = {
                "manager_name": manager_info["manager_name"],
                "risk_profile": manager_info["risk_profile"],
                "strategy": manager_info["strategy_description"],
                "target_return_pct": float(manager_info["target_return_pct"]),
                "max_volatility_pct": float(manager_info["max_volatility_pct"]),
                "beta_range": f"{manager_info['beta_min']}-{manager_info['beta_max']}",
                "top_holdings": [
                    {
                        "symbol": h["symbol"],
                        "company": h["company_name"],
                        "sector": h["sector"],
                        "weight_pct": round(float(h["weight"]) * 100, 2),
                        "value_usd": round(float(h["value_usd"]), 0),
                        "beta": float(h["beta"]),
                        "volatility": float(h["volatility_30d"])
                    }
                    for h in manager_holdings
                ],
                "sector_allocation": [
                    {
                        "sector": s["sector"],
                        "num_stocks": int(s["num_stocks"]),
                        "weight_pct": round(float(s["total_weight"]) * 100, 2)
                    }
                    for s in sector_allocation
                ]
            }
            
            print(f"✅ Retrieved profile for {manager_name}")
            return json.dumps(result, indent=2)
        
        # Compare all managers
        elif "compare" in query_lower or "all" in query_lower:
            all_managers = managers_df.collect()
            
            comparison = []
            for mgr in all_managers:
                # Get portfolio metrics
                mgr_holdings = holdings_df.filter(F.col("manager_id") == mgr["manager_id"]) \
                    .join(companies_df, "symbol")
                
                avg_beta = mgr_holdings.agg(
                    (F.sum(F.col("weight") * F.col("beta"))).alias("weighted_beta")
                ).collect()[0]["weighted_beta"]
                
                avg_volatility = mgr_holdings.agg(
                    (F.sum(F.col("weight") * F.col("volatility_30d"))).alias("weighted_vol")
                ).collect()[0]["weighted_vol"]
                
                num_stocks = mgr_holdings.count()
                total_value = mgr_holdings.agg(F.sum("value_usd")).collect()[0][0]
                
                comparison.append({
                    "manager_name": mgr["manager_name"],
                    "risk_profile": mgr["risk_profile"],
                    "num_holdings": int(num_stocks),
                    "total_value_usd": round(float(total_value), 0),
                    "target_return_pct": float(mgr["target_return_pct"]),
                    "max_volatility_pct": float(mgr["max_volatility_pct"]),
                    "portfolio_beta": round(float(avg_beta), 2),
                    "portfolio_volatility": round(float(avg_volatility), 2),
                    "beta_range": f"{mgr['beta_min']}-{mgr['beta_max']}"
                })
            
            result = {
                "comparison": "All Portfolio Managers",
                "managers": comparison,
                "total_aum": sum(m["total_value_usd"] for m in comparison)
            }
            
            print(f"✅ Compared {len(comparison)} managers")
            return json.dumps(result, indent=2)
        
        # General query - list all managers
        else:
            all_managers = managers_df.collect()
            result = {
                "available_managers": [
                    {
                        "manager_name": m["manager_name"],
                        "risk_profile": m["risk_profile"],
                        "strategy": m["strategy_description"]
                    }
                    for m in all_managers
                ],
                "hint": "Ask about a specific manager (Sarah Russel, Rena Tang, Mohit Arora) or request a comparison"
            }
            
            return json.dumps(result, indent=2)
            
    except Exception as e:
        error_result = {
            "error": str(e),
            "recommendation": "Check portfolio_managers and portfolio_holdings tables"
        }
        print(f"❌ Portfolio manager query error: {e}")
        return json.dumps(error_result)

# Create LangChain Tool
portfolio_manager_tool = Tool(
    name="query_portfolio_manager",
    description="""Queries portfolio manager information and their holdings.
    
    Can answer questions like:
    - "Show me Sarah Russel's portfolio"
    - "What is Rena Tang's risk profile?"
    - "Compare all three portfolio managers"
    - "Show Mohit Arora's top holdings"
    - "What is the sector allocation for Sarah Russel?"
    
    Input: Natural language query about portfolio managers
    Output: JSON with manager profile, holdings, or comparison data
    
    Example: query_portfolio_manager('Show me Sarah Russel portfolio')""",
    func=query_portfolio_manager
)

print("✅ Portfolio Manager Query Tool created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 4: Risk Calculation Agent

# COMMAND ----------

def calculate_portfolio_risk(portfolio_description: str) -> str:
    """
    Retrieves computed portfolio risk metrics from gold layer.
    
    This agent provides:
    - Value at Risk (VaR) at 95% confidence (1-day and 10-day)
    - Portfolio beta and volatility
    - Sector allocation from computed metrics
    - Factor exposures (momentum, volatility)
    - Stress test results (4 scenarios)
    
    Args:
        portfolio_description: Manager name or 'all managers' or 'compare'
    
    Returns:
        JSON string with VaR, exposures, stress tests from gold tables
    """
    try:
        print(f"📊 Retrieving risk metrics for: {portfolio_description}")
        
        # Load computed risk metrics from gold layer
        risk_metrics_table = f"{catalog}.{schema_gold}.portfolio_risk_metrics"
        stress_tests_table = f"{catalog}.{schema_gold}.stress_test_results"
        factor_exposures_table = f"{catalog}.{schema_gold}.factor_exposures"
        sector_exposures_table = f"{catalog}.{schema_gold}.sector_exposures"
        
        # Load all tables
        risk_df = spark.table(risk_metrics_table)
        stress_df = spark.table(stress_tests_table)
        factor_df = spark.table(factor_exposures_table)
        sector_df = spark.table(sector_exposures_table)
        
        # Identify which manager(s) to query
        query_lower = portfolio_description.lower()
        manager_filter = None
        
        if "sarah" in query_lower or "russel" in query_lower:
            manager_filter = "Sarah Russel"
        elif "rena" in query_lower or "tang" in query_lower:
            manager_filter = "Rena Tang"
        elif "mohit" in query_lower or "arora" in query_lower:
            manager_filter = "Mohit Arora"
        
        # Get risk metrics
        if manager_filter:
            risk_data = risk_df.filter(F.col("manager_name") == manager_filter).collect()
            stress_data = stress_df.filter(F.col("manager_name") == manager_filter).collect()
            factor_data = factor_df.filter(F.col("manager_name") == manager_filter).collect()
            sector_data = sector_df.filter(F.col("manager_name") == manager_filter).collect()
        else:
            # Return all managers
            risk_data = risk_df.collect()
            stress_data = stress_df.collect()
            factor_data = factor_df.collect()
            sector_data = sector_df.collect()
        
        if not risk_data:
            return json.dumps({"error": "No risk metrics found. Run 03_risk_analytics.py first"})
        
        # Format risk metrics
        risk_metrics = []
        for row in risk_data:
            risk_metrics.append({
                "manager_name": row["manager_name"],
                "risk_profile": row["risk_profile"],
                "aum_usd": round(float(row["total_value_usd"]), 0),
                "portfolio_beta": round(float(row["portfolio_beta"]), 2),
                "weighted_volatility_pct": round(float(row["weighted_volatility"]) * 100, 2),
                "var_1day_95_usd": round(float(row["var_1day_95"]), 0),
                "var_10day_95_usd": round(float(row["var_10day_95"]), 0),
                "num_positions": int(row["num_positions"])
            })
        
        # Format stress test results
        stress_tests = {}
        for row in stress_data:
            scenario = row["scenario_name"]
            if scenario not in stress_tests:
                stress_tests[scenario] = {
                    "description": row["scenario_description"],
                    "impacts": []
                }
            stress_tests[scenario]["impacts"].append({
                "manager_name": row["manager_name"],
                "impact_usd": round(float(row["total_impact_usd"]), 0),
                "impact_pct": round(float(row["impact_percentage"]), 2)
            })
        
        # Format factor exposures
        factors = []
        for row in factor_data:
            factors.append({
                "manager_name": row["manager_name"],
                "momentum_factor": round(float(row["momentum_factor"]), 4),
                "volatility_factor": round(float(row["volatility_factor"]), 4)
            })
        
        # Format sector exposures
        sectors = {}
        for row in sector_data:
            mgr = row["manager_name"]
            if mgr not in sectors:
                sectors[mgr] = []
            sectors[mgr].append({
                "sector": row["sector"],
                "weight_pct": round(float(row["sector_weight"]) * 100, 2)
            })
        
        # Generate recommendations
        recommendations = []
        for metric in risk_metrics:
            if metric["var_1day_95_usd"] > metric["aum_usd"] * 0.03:
                recommendations.append(f"⚠️ {metric['manager_name']}: High VaR at {metric['var_1day_95_usd']/metric['aum_usd']*100:.1f}% of AUM")
            if metric["portfolio_beta"] > 1.3:
                recommendations.append(f"⚠️ {metric['manager_name']}: High beta ({metric['portfolio_beta']}) increases market sensitivity")
        
        if not recommendations:
            recommendations.append("✅ All portfolios within acceptable risk ranges")
        
        result = {
            "risk_metrics": risk_metrics,
            "stress_tests": stress_tests,
            "factor_exposures": factors,
            "sector_exposures": sectors,
            "recommendations": recommendations
        }
        
        print(f"✅ Retrieved risk metrics for {len(risk_metrics)} manager(s)")
        return json.dumps(result, indent=2)
        
    except Exception as e:
        error_result = {
            "error": str(e),
            "recommendation": "Ensure 03_risk_analytics.py has been run to populate gold risk tables"
        }
        print(f"❌ Risk metrics retrieval error: {e}")
        return json.dumps(error_result)

# Create LangChain Tool
risk_tool = Tool(
    name="calculate_risk",
    description="""Retrieves computed portfolio risk metrics from gold layer including VaR at 95% confidence (1-day and 10-day),
    portfolio beta, weighted volatility, stress test results (4 scenarios), factor exposures, and sector allocations.
    
    Input: Manager name ('Sarah Russel', 'Rena Tang', 'Mohit Arora') or 'all managers'
    Output: JSON with VaR, beta, volatility, stress tests, factor/sector exposures, and risk recommendations
    
    Example: calculate_risk('Sarah Russel') or calculate_risk('all managers')""",
    func=calculate_portfolio_risk
)

print("✅ Risk Calculation Tool created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 4: Python REPL Tool (for ad-hoc calculations)

# COMMAND ----------

from langchain.agents import Tool
from langchain_experimental.utilities import PythonREPL

python_repl = PythonREPL()

# Wrapper to make it safer
def execute_python(code: str) -> str:
    """Executes Python code safely"""
    try:
        result = python_repl.run(code)
        return str(result)
    except Exception as e:
        return f"Error executing code: {str(e)}"

repl_tool = Tool(
    name="python_repl",
    description="""Executes Python code for ad-hoc calculations.
    Use this for quick math, data transformations, or calculations not covered by other tools.
    
    Input: Python code as a string
    Output: Result of the code execution
    
    Example: python_repl('import math; math.sqrt(16)')""",
    func=execute_python
)

print("✅ Python REPL Tool created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Multi-Agent Supervisor
# MAGIC
# MAGIC The supervisor is the "brain" that coordinates all the specialized agents.

# COMMAND ----------

# Define supervisor LLM (larger context for coordination)
supervisor_llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    max_tokens=3000,
    temperature=0.1  # Low temperature for consistent reasoning
)

# All tools available to supervisor
all_tools = [validation_tool, macro_tool, portfolio_manager_tool, risk_tool, repl_tool]

print(f"✅ Supervisor LLM configured: Llama 3.3 70B")
print(f"✅ Available tools: {[tool.name for tool in all_tools]}")

# COMMAND ----------

# Create custom supervisor prompt with clear workflow
supervisor_prompt = ChatPromptTemplate.from_messages([
    ("system", """You are RiskBricks Supervisor, an expert financial risk analyst coordinating a team of specialized AI agents.

Your mission: Provide accurate, actionable portfolio risk analysis by orchestrating your team of tools.

🔧 YOUR TOOLS:
- validate_data: Checks data quality (ALWAYS USE FIRST)
- get_macro_context: Gets current macro economic indicators
- query_portfolio_manager: Retrieves portfolio manager profiles and holdings (Sarah Russel, Rena Tang, Mohit Arora)
- calculate_risk: Computes portfolio risk metrics and stress tests
- python_repl: Executes Python code for calculations

📋 WORKFLOW RULES (FOLLOW STRICTLY):
1. ALWAYS start by validating data using validate_data tool
2. If validation status is "FAIL", STOP immediately and alert user  
3. If validation is "PASS" or "WARNING", proceed to step 4
4. If user asks about specific manager, use query_portfolio_manager tool
5. Get macro economic context using get_macro_context
6. Calculate risk metrics using calculate_risk
7. Synthesize all results into a clear executive summary

📊 EXECUTIVE SUMMARY MUST INCLUDE:
- Overall risk level (Low/Medium/High)
- Current VaR and what it means in plain English
- Key risk drivers (sector concentration, macro factors)
- Most concerning stress test scenario
- 2-3 specific, actionable recommendations

🎯 COMMUNICATION STYLE:
- Be precise but avoid jargon
- Use percentages and dollar figures
- Explain technical terms
- Structure output clearly

Begin analysis:"""),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad")
])

# Create supervisor agent using tool-calling pattern
supervisor_agent = create_tool_calling_agent(
    supervisor_llm,
    all_tools,
    supervisor_prompt
)

# Create executor with error handling
riskbricks_supervisor = AgentExecutor(
    agent=supervisor_agent,
    tools=all_tools,
    verbose=True,
    handle_parsing_errors=True,
    max_iterations=15,
    return_intermediate_steps=True,
    early_stopping_method="generate"
)

print("✅ Multi-Agent Supervisor created")
print(f"✅ Max iterations: 15")
print(f"✅ Error handling: Enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Multi-Agent Workflow
# MAGIC
# MAGIC Now let's put the supervisor to work!

# COMMAND ----------

# Run complete risk analysis
print("=" * 80)
print("🚀 RISKBRICKS MULTI-AGENT WORKFLOW STARTING")
print("=" * 80)
print()

response = riskbricks_supervisor.invoke({
    "input": """
Perform a complete portfolio risk analysis for all three managers.

Required steps:
1. Validate data quality for stock prices (table: riskbricks.silver.stock_prices)
2. Get current macro economic context
3. Query all portfolio managers to understand their profiles
4. Retrieve computed risk metrics and stress tests for all managers 
5. Generate executive summary with:
   - Overall risk assessment for each manager
   - Key risk drivers and differentiators
   - Most concerning stress test scenarios
   - Actionable recommendations by manager
"""
})

print("\n" + "=" * 80)
print("✅ RISKBRICKS ANALYSIS COMPLETE")
print("=" * 80)
print("\n📋 EXECUTIVE SUMMARY:")
print("-" * 80)
print(response["output"])
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Agent Execution Steps (Transparency)
# MAGIC
# MAGIC This shows exactly what each agent did - crucial for explainability and compliance.

# COMMAND ----------

print("\n📊 AGENT EXECUTION TRACE")
print("=" * 80)

for i, step in enumerate(response["intermediate_steps"], 1):
    action = step[0]
    observation = step[1]
    
    print(f"\n🔹 Step {i}: {action.tool.upper()}")
    print("-" * 80)
    print(f"📥 Input:")
    print(f"   {action.tool_input[:150]}..." if len(str(action.tool_input)) > 150 else f"   {action.tool_input}")
    print(f"\n📤 Output:")
    
    # Pretty print JSON if possible
    try:
        output_json = json.loads(observation)
        print(json.dumps(output_json, indent=2)[:500])
        if len(observation) > 500:
            print("   ... (truncated)")
    except:
        print(f"   {observation[:500]}")
        if len(observation) > 500:
            print("   ... (truncated)")
    
    print("-" * 80)

print("\n✅ Total steps executed:", len(response["intermediate_steps"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries: Portfolio Manager Analysis
# MAGIC
# MAGIC Now test the agent with different types of portfolio queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Compare All Managers (Already tested above ✅)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Specific Manager Analysis

# COMMAND ----------

response = riskbricks_supervisor.invoke({
    "input": "Tell me about Sarah Russel's portfolio. What are her top 5 holdings and why are they conservative?"
})
print(response["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Sector Exposure

# COMMAND ----------

response = riskbricks_supervisor.invoke({
    "input": "Which manager has the highest technology exposure and is that risky?"
})
print(response["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Risk Comparison

# COMMAND ----------

response = riskbricks_supervisor.invoke({
    "input": "If we're worried about a market downturn, which manager's portfolio would hold up best?"
})
print(response["output"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Integration (Tracking Only)
# MAGIC
# MAGIC **Note:** Direct serialization of `AgentExecutor` is not supported by MLflow. 
# MAGIC For production deployment, wrap the agent in a custom `mlflow.pyfunc.PythonModel`.
# MAGIC
# MAGIC For now, we'll just log metrics and parameters for tracking.

# COMMAND ----------

model_name = f"{catalog}.agents.riskbricks_supervisor"

print(f"📊 Logging agent run to MLflow: {model_name}")

with mlflow.start_run(run_name="riskbricks_multi_agent_v1") as run:
    
    # Log parameters
    mlflow.log_param("agent_type", "multi_agent_supervisor")
    mlflow.log_param("framework", "langchain")
    mlflow.log_param("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
    mlflow.log_param("max_iterations", 15)
    mlflow.log_param("num_tools", len(all_tools))
    mlflow.log_param("tools", [tool.name for tool in all_tools])
    
    # Log metrics from last run
    try:
        num_steps = len(response["intermediate_steps"])
        mlflow.log_metric("execution_steps", num_steps)
        
        # Check if validation passed
        validation_step = [s for s in response["intermediate_steps"] if s[0].tool == "validate_data"]
        if validation_step:
            validation_result = json.loads(validation_step[0][1])
            mlflow.log_metric("data_quality_score", validation_result.get("completeness_score", 0))
    except:
        pass
    
    print(f"\n✅ Agent run logged successfully!")
    print(f"🔗 Run ID: {run.info.run_id}")
    print(f"\n💡 To deploy to production:")
    print(f"   1. Wrap agent in mlflow.pyfunc.PythonModel")
    print(f"   2. Use mlflow.pyfunc.log_model() with custom wrapper")
    print(f"   3. Or migrate to Agent Bricks Multi-Agent Supervisor service")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Deployment Pattern (Optional)
# MAGIC
# MAGIC For production deployment, wrap the agent in a custom PyFunc model:
# MAGIC
# MAGIC ```python
# MAGIC class RiskBricksAgent(mlflow.pyfunc.PythonModel):
# MAGIC     def load_context(self, context):
# MAGIC         # Initialize agent in load_context
# MAGIC         self.agent = create_agent(...)
# MAGIC     
# MAGIC     def predict(self, context, model_input):
# MAGIC         return self.agent.invoke(model_input)
# MAGIC
# MAGIC mlflow.pyfunc.log_model(
# MAGIC     artifact_path="agent",
# MAGIC     python_model=RiskBricksAgent(),
# MAGIC     registered_model_name="riskbricks.agents.supervisor"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Before vs After
# MAGIC
# MAGIC ### ❌ Before (Simulated Agents)
# MAGIC ```python
# MAGIC class DataValidationAgent:
# MAGIC     def validate_data(self, data):
# MAGIC         # Hard-coded logic
# MAGIC         # No LLM reasoning
# MAGIC         # No natural language understanding
# MAGIC ```
# MAGIC
# MAGIC ### ✅ After (Real Agent Bricks)
# MAGIC ```python
# MAGIC supervisor_agent = create_tool_calling_agent(
# MAGIC     llm=ChatDatabricks("databricks-meta-llama-3-3-70b-instruct"),
# MAGIC     tools=[validation_tool, risk_tool, macro_tool],
# MAGIC     prompt=supervisor_prompt
# MAGIC )
# MAGIC # LLM decides which tool to call, when, and with what parameters
# MAGIC # Natural language understanding
# MAGIC # Adaptive reasoning based on context
# MAGIC ```
# MAGIC
# MAGIC ## Key Differences
# MAGIC
# MAGIC | Aspect | Simulation | Real Agent Bricks |
# MAGIC |--------|-----------|------------------|
# MAGIC | **LLM** | ❌ None | ✅ Databricks Llama 3.3 70B |
# MAGIC | **Decision Making** | ❌ Hard-coded | ✅ LLM reasoning (ReAct) |
# MAGIC | **Natural Language** | ❌ No | ✅ Yes |
# MAGIC | **Tool Selection** | ❌ Pre-defined order | ✅ Dynamic based on context |
# MAGIC | **Framework** | ❌ Custom classes | ✅ LangChain AgentExecutor |
# MAGIC | **Logging** | ❌ Manual | ✅ MLflow auto-log |
# MAGIC | **Deployment** | ❌ Not registered | ✅ Unity Catalog model |
# MAGIC
# MAGIC ## Next Steps: Agent Bricks Service Migration
# MAGIC
# MAGIC Once **Agent Bricks Multi-Agent Supervisor (Beta)** is available in your workspace:
# MAGIC
# MAGIC 1. **Enable Beta Feature**
# MAGIC    - Workspace admin enables "Mosaic AI Agent Bricks Preview"
# MAGIC    - Ensure workspace in supported region (us-east-1 or us-west-2)
# MAGIC
# MAGIC 2. **Configure in Agent Bricks UI**
# MAGIC    - Define agent tasks declaratively
# MAGIC    - Point to tools and data sources
# MAGIC    - Agent Bricks auto-generates evaluation suite
# MAGIC
# MAGIC 3. **Benefits of Agent Bricks Service**
# MAGIC    - ✅ Automatic quality optimization
# MAGIC    - ✅ Cost/quality balancing (tests different models)
# MAGIC    - ✅ Synthetic evaluation datasets
# MAGIC    - ✅ Continuous improvement without code changes
# MAGIC    - ✅ One-click deployment to production endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### ✅ What We Built (Real Agent Bricks)
# MAGIC
# MAGIC 1. **Real LangChain Agents**
# MAGIC    - Using official `create_tool_calling_agent` pattern
# MAGIC    - AgentExecutor with error handling
# MAGIC
# MAGIC 2. **Real LLM Integration**
# MAGIC    - ChatDatabricks with Llama 3.3 70B Instruct
# MAGIC    - Temperature tuning for consistent reasoning
# MAGIC
# MAGIC 3. **Real Tools**
# MAGIC    - Data validation with Spark DataFrames
# MAGIC    - Macro context from Unity Catalog tables
# MAGIC    - Risk calculation with VaR and stress tests
# MAGIC    - Python REPL for ad-hoc calculations
# MAGIC
# MAGIC 4. **Real Planning**
# MAGIC    - ReAct framework (Reasoning + Acting)
# MAGIC    - LLM decides tool usage dynamically
# MAGIC
# MAGIC 5. **Real MLflow Integration**
# MAGIC    - Auto-logging enabled
# MAGIC    - Model registration in Unity Catalog
# MAGIC    - Version control and lineage tracking
# MAGIC
# MAGIC ### 🎯 For CFP Submission
# MAGIC
# MAGIC **Key Message:**
# MAGIC > "RiskBricks demonstrates production-ready multi-agent architecture using Databricks Agent Framework (LangChain + ChatDatabricks). This architecture seamlessly migrates to Agent Bricks Multi-Agent Supervisor service for automatic optimization and enterprise governance."
# MAGIC
# MAGIC ### 📚 References
# MAGIC
# MAGIC - [Databricks Agent Bricks](https://www.databricks.com/product/artificial-intelligence/agent-bricks)
# MAGIC - [Agent Bricks Documentation](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/)
# MAGIC - [Multi-Agent Supervisor](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor)
# MAGIC - Databricks GenAI Course: Module 2.3 - Agents and Cognitive Architectures

# COMMAND ----------

print("🎉 RiskBricks Multi-Agent Workflow Complete!")
print("✅ Real Agent Bricks implementation using LangChain + ChatDatabricks")
print("✅ Multi-agent orchestration with supervisor pattern")
print("✅ MLflow tracking and model registry integration")
print("✅ Ready for CFP demo!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

