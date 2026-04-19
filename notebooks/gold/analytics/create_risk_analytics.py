# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Risk Analytics - Silver to Gold Layer
# MAGIC
# MAGIC **Purpose**: Compute portfolio risk metrics, VaR, stress tests, and factor exposures
# MAGIC
# MAGIC **Inputs**:
# MAGIC - `riskbricks.silver.stock_prices`
# MAGIC - `riskbricks.silver.macro_indicators`
# MAGIC - `riskbricks.gold.portfolio_managers`
# MAGIC - `riskbricks.gold.portfolio_holdings`
# MAGIC
# MAGIC **Outputs**:
# MAGIC - `riskbricks.gold.portfolio_risk_metrics` (VaR, volatility, beta)
# MAGIC - `riskbricks.gold.stress_test_results` (scenario impacts)
# MAGIC
# MAGIC **Run Sequence**: After `02_data_validation.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import mlflow
import mlflow.pyfunc

# COMMAND ----------

# Configuration
catalog = "riskbricks"
silver_schema = "silver"
gold_schema = "gold"

print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schemas: {silver_schema} → {gold_schema}")

# Risk calculation parameters
CONFIDENCE_LEVEL = 0.95  # 95% VaR
LOOKBACK_DAYS = 252      # 1 year of trading days
VaR_HORIZON = 1          # 1-day VaR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load Data

# COMMAND ----------

print("📊 Loading validated data from Silver layer...")

# Load silver tables
stock_prices = spark.table(f"{catalog}.{silver_schema}.stock_prices")
macro_indicators = spark.table(f"{catalog}.{silver_schema}.macro_indicators")

# Load portfolio data from Gold
portfolio_managers = spark.table(f"{catalog}.{gold_schema}.portfolio_managers")
portfolio_holdings = spark.table(f"{catalog}.{gold_schema}.portfolio_holdings")
company_universe = spark.table(f"{catalog}.{gold_schema}.company_universe")

print(f"✅ Stock prices: {stock_prices.count():,} records")
print(f"✅ Macro indicators: {macro_indicators.count():,} records")
print(f"✅ Portfolio managers: {portfolio_managers.count()}")
print(f"✅ Portfolio holdings: {portfolio_holdings.count():,} positions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Calculate Stock Returns

# COMMAND ----------

print("📈 Computing daily returns...")

# Calculate daily returns for all stocks
window_spec = Window.partitionBy("symbol").orderBy("date")

stock_returns = stock_prices \
    .withColumn("prev_close", lag("close").over(window_spec)) \
    .withColumn("daily_return",
                when(col("prev_close").isNotNull(),
                     (col("close") - col("prev_close")) / col("prev_close"))
                .otherwise(0)) \
    .filter(col("prev_close").isNotNull()) \
    .select("symbol", "date", "close", "daily_return", "volume")

print(f"✅ Computed returns for {stock_returns.select('symbol').distinct().count()} stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Calculate Stock Volatility and Beta

# COMMAND ----------

print("📊 Computing volatility (30-day, 90-day) and beta...")

# 30-day rolling volatility
window_30d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
window_90d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-89, 0)

stock_metrics = stock_returns \
    .withColumn("volatility_30d", stddev("daily_return").over(window_30d) * sqrt(lit(252))) \
    .withColumn("volatility_90d", stddev("daily_return").over(window_90d) * sqrt(lit(252))) \
    .withColumn("avg_volume_30d", avg("volume").over(window_30d))

# Get latest metrics for each stock
latest_metrics = stock_metrics \
    .withColumn("row_num",
                row_number().over(
                    Window.partitionBy("symbol").orderBy(desc("date"))
                )) \
    .filter(col("row_num") == 1) \
    .select(
        "symbol",
        "date",
        "close",
        "volatility_30d",
        "volatility_90d",
        "avg_volume_30d"
    )

# For beta, we'd need a market index (S&P 500 proxy)
# For now, use SPY or compute average of all stocks as market proxy
print("   Computing beta against market (using portfolio weighted average as proxy)...")

# Simple market proxy: equal-weighted average of all stock returns
market_returns = stock_returns \
    .groupBy("date") \
    .agg(avg("daily_return").alias("market_return"))

# Join stock returns with market returns
returns_with_market = stock_returns.join(market_returns, "date")

# Calculate beta for each stock (using recent data)
recent_date = stock_returns.agg(max("date")).collect()[0][0]
lookback_date = recent_date - timedelta(days=LOOKBACK_DAYS)

beta_calc = returns_with_market \
    .filter(col("date") >= lookback_date) \
    .groupBy("symbol") \
    .agg(
        (covar_pop("daily_return", "market_return") / variance("market_return")).alias("beta"),
        count("*").alias("num_observations")
    ) \
    .filter(col("num_observations") >= 50)  # Require minimum data points

# Combine metrics with beta
stock_metrics_final = latest_metrics \
    .join(beta_calc.select("symbol", "beta"), "symbol", "left") \
    .withColumn("beta", coalesce(col("beta"), lit(1.0))) \
    .withColumn("computed_at", current_timestamp())

print(f"✅ Computed metrics for {stock_metrics_final.count()} stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💼 Calculate Portfolio Risk Metrics

# COMMAND ----------

print("💼 Computing portfolio-level risk metrics...")

# Join holdings with stock metrics and company data
# Drop columns from holdings to avoid ambiguity (we'll use data from stock_metrics and company_universe)
portfolio_holdings_clean = portfolio_holdings.drop("sector", "volatility_30d", "beta")

portfolio_risk = portfolio_holdings_clean \
    .join(stock_metrics_final, "symbol") \
    .join(company_universe.select("symbol", "company_name", "sector"), "symbol") \
    .withColumn("position_volatility", col("volatility_30d") * col("weight")) \
    .withColumn("position_beta_contribution", col("beta") * col("weight"))

# Aggregate by portfolio
portfolio_summary = portfolio_risk \
    .groupBy("portfolio_id", "manager_id") \
    .agg(
        sum("weight").alias("total_weight"),
        sum("value_usd").alias("aum_usd"),  # Renamed from total_value_usd
        sum("position_volatility").alias("weighted_volatility_pct"),  # Renamed for clarity
        sum("position_beta_contribution").alias("portfolio_beta"),
        avg("volatility_30d").alias("avg_stock_volatility"),
        count("*").alias("num_positions")
    )

# Calculate VaR (simplified parametric VaR)
# VaR = Portfolio Value * Volatility * Z-score
z_score = 1.645  # 95% confidence (one-tailed)

portfolio_var = portfolio_summary \
    .withColumn("var_1day_95_usd",  # Renamed for clarity
                col("aum_usd") * col("weighted_volatility_pct") * lit(z_score) / sqrt(lit(252))) \
    .withColumn("var_10day_95_usd",  # Renamed for clarity
                col("aum_usd") * col("weighted_volatility_pct") * lit(z_score) * sqrt(lit(10)) / sqrt(lit(252))) \
    .withColumn("computed_at", current_timestamp())

# Join with manager names
portfolio_var_final = portfolio_var \
    .join(
        portfolio_managers.select("manager_id", "manager_name", "risk_profile"),
        "manager_id"
    )

print(f"✅ Computed VaR for {portfolio_var_final.count()} portfolios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Stress Test Scenarios

# COMMAND ----------

print("🎯 Running stress test scenarios...")

# Define stress scenarios
stress_scenarios = [
    {
        "scenario_name": "Market Crash -20%",
        "market_shock": -0.20,
        "volatility_multiplier": 2.0,
        "description": "Severe market downturn with volatility spike"
    },
    {
        "scenario_name": "Tech Sector Drawdown -30%",
        "sector_shock": {"Technology": -0.30},
        "volatility_multiplier": 1.5,
        "description": "Technology sector specific crash"
    },
    {
        "scenario_name": "Interest Rate Spike +200bp",
        "market_shock": -0.10,
        "high_beta_penalty": -0.15,
        "description": "Federal Reserve emergency rate hike"
    },
    {
        "scenario_name": "Recession Scenario",
        "market_shock": -0.15,
        "volatility_multiplier": 1.8,
        "cyclical_shock": -0.25,
        "description": "Economic recession with cyclical sector impact"
    }
]

# Calculate portfolio impacts for each scenario
stress_results = []

for scenario in stress_scenarios:
    scenario_name = scenario["scenario_name"]
    print(f"   Running: {scenario_name}...")
    
    # Apply shocks to portfolio holdings
    # Initialize shock_return column with 0
    stressed_portfolio = portfolio_risk.withColumn("shock_return", lit(0.0))
    
    # Market-wide shock
    if "market_shock" in scenario:
        stressed_portfolio = stressed_portfolio \
            .withColumn("shock_return", col("shock_return") + lit(scenario["market_shock"]))
    
    # Sector-specific shock
    if "sector_shock" in scenario:
        for sector, shock in scenario["sector_shock"].items():
            stressed_portfolio = stressed_portfolio \
                .withColumn("shock_return",
                           when(col("sector") == sector, col("shock_return") + lit(shock))
                           .otherwise(col("shock_return")))
    
    # Beta-based shock
    if "high_beta_penalty" in scenario:
        stressed_portfolio = stressed_portfolio \
            .withColumn("shock_return",
                       col("shock_return") +
                       when(col("beta") > 1.2, lit(scenario["high_beta_penalty"]))
                       .otherwise(lit(0.0)))
    
    # Calculate portfolio impact
    portfolio_impact = stressed_portfolio \
        .withColumn("position_impact", col("value_usd") * col("shock_return")) \
        .groupBy("portfolio_id", "manager_id") \
        .agg(
            sum("position_impact").alias("total_impact_usd"),
            sum("value_usd").alias("total_value_usd")
        ) \
        .withColumn("impact_percentage", col("total_impact_usd") / col("total_value_usd") * 100) \
        .withColumn("scenario_name", lit(scenario_name)) \
        .withColumn("scenario_description", lit(scenario["description"])) \
        .withColumn("computed_at", current_timestamp())
    
    # Join with manager info
    scenario_result = portfolio_impact \
        .join(portfolio_managers.select("manager_id", "manager_name"), "manager_id")
    
    stress_results.append(scenario_result)

# Union all stress test results
all_stress_results = stress_results[0]
for result in stress_results[1:]:
    all_stress_results = all_stress_results.union(result)

print(f"✅ Completed {len(stress_scenarios)} stress test scenarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏷️ Calculate Factor Exposures

# COMMAND ----------

print("🏷️ Computing factor exposures...")

# Get recent returns for factor analysis
recent_date = stock_returns.agg(max("date")).collect()[0][0]
factor_lookback_date = recent_date - timedelta(days=90)

recent_returns = stock_returns \
    .filter(col("date") >= factor_lookback_date) \
    .join(company_universe.select("symbol", "sector"), "symbol")

# Calculate factor proxies
window_symbol = Window.partitionBy("symbol").orderBy("date")

factor_data = recent_returns \
    .withColumn("momentum_20d",
                avg("daily_return").over(
                    Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, -1)
                )) \
    .withColumn("volatility_factor",
                stddev("daily_return").over(
                    Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
                ))

# Get latest factor values
latest_factors = factor_data \
    .withColumn("row_num",
                row_number().over(
                    Window.partitionBy("symbol").orderBy(desc("date"))
                )) \
    .filter(col("row_num") == 1) \
    .select(
        "symbol",
        "sector",
        "momentum_20d",
        "volatility_factor"
    )

# Calculate portfolio factor exposures
# Use the same cleaned holdings without sector to avoid ambiguity
portfolio_factors = portfolio_holdings_clean \
    .join(latest_factors, "symbol") \
    .withColumn("momentum_exposure", col("weight") * col("momentum_20d")) \
    .withColumn("volatility_exposure", col("weight") * col("volatility_factor")) \
    .groupBy("portfolio_id", "manager_id") \
    .agg(
        sum("momentum_exposure").alias("momentum_factor"),
        sum("volatility_exposure").alias("volatility_factor")
    ) \
    .withColumn("computed_at", current_timestamp())

# Sector exposures
sector_exposures = portfolio_holdings_clean \
    .join(company_universe.select("symbol", "sector"), "symbol") \
    .groupBy("portfolio_id", "manager_id", "sector") \
    .agg(sum("weight").alias("sector_weight")) \
    .withColumn("computed_at", current_timestamp())

print(f"✅ Factor exposures computed for {portfolio_factors.count()} portfolios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Gold Layer

# COMMAND ----------

print("💾 Saving risk metrics to Gold layer...")

# 1. Portfolio Risk Metrics (VaR, volatility, beta)
portfolio_var_final \
    .select(
        "portfolio_id",
        "manager_id",
        "manager_name",
        "risk_profile",
        "aum_usd",
        "weighted_volatility_pct",
        "portfolio_beta",
        "avg_stock_volatility",
        "num_positions",
        "var_1day_95_usd",
        "var_10day_95_usd",
        "computed_at"
    ) \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{gold_schema}.portfolio_risk_metrics")

print(f"✅ Saved portfolio_risk_metrics")

# 2. Stress Test Results
all_stress_results \
    .select(
        "portfolio_id",
        "manager_id",
        "manager_name",
        "scenario_name",
        "scenario_description",
        col("total_value_usd").alias("aum_usd"),  # Rename for consistency
        "total_impact_usd",
        col("impact_percentage").alias("impact_pct"),  # Rename for consistency
        "computed_at"
    ) \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{gold_schema}.stress_test_results")

print(f"✅ Saved stress_test_results")

# 3. Factor Exposures
portfolio_factors \
    .join(portfolio_managers.select("manager_id", "manager_name"), "manager_id") \
    .select(
        "portfolio_id",
        "manager_id",
        "manager_name",
        "momentum_factor",
        "volatility_factor",
        "computed_at"
    ) \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \


# 4. Sector Exposures
sector_exposures \
    .join(portfolio_managers.select("manager_id", "manager_name"), "manager_id") \
    .select(
        "portfolio_id",
        "manager_id",
        "manager_name",
        "sector",
        col("sector_weight").alias("sector_weight_pct"),  # Rename for clarity
        "computed_at"
    ) \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{gold_schema}.sector_exposures")

print(f"✅ Saved sector_exposures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Risk Analytics Report

# COMMAND ----------

print("\n" + "="*70)
print("📊 PORTFOLIO RISK ANALYTICS REPORT")
print("="*70 + "\n")

# Portfolio Risk Summary
print("💼 PORTFOLIO RISK METRICS:")
print("-" * 70)
risk_df = spark.table(f"{catalog}.{gold_schema}.portfolio_risk_metrics").toPandas()

for _, row in risk_df.iterrows():
    print(f"\n👤 {row['manager_name']} ({row['risk_profile']} Risk)")
    print(f"   AUM: ${row['aum_usd']:,.0f}")
    print(f"   Portfolio Beta: {row['portfolio_beta']:.2f}")
    print(f"   Weighted Volatility: {row['weighted_volatility_pct']:.2f}%")
    print(f"   1-Day VaR (95%): ${row['var_1day_95_usd']:,.0f}")
    print(f"   10-Day VaR (95%): ${row['var_10day_95_usd']:,.0f}")
    print(f"   Positions: {int(row['num_positions'])}")

# Stress Test Summary
print("\n" + "-" * 70)
print("🎯 STRESS TEST RESULTS:")
print("-" * 70)
stress_df = spark.table(f"{catalog}.{gold_schema}.stress_test_results").toPandas()

for scenario in stress_df['scenario_name'].unique():
    print(f"\n📉 {scenario}")
    scenario_data = stress_df[stress_df['scenario_name'] == scenario]
    for _, row in scenario_data.iterrows():
        print(f"   {row['manager_name']}: {row['impact_pct']:.2f}% (${row['total_impact_usd']:,.0f})")

print("\n" + "="*70)
print("✅ RISK ANALYTICS COMPLETE - Gold layer ready for AI agents")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Visualize Risk Metrics

# COMMAND ----------

print("📈 Portfolio Risk Comparison:")
display(spark.sql(f"""
    SELECT
        manager_name,
        risk_profile,
        ROUND(aum_usd, 0) as AUM,
        ROUND(portfolio_beta, 2) as Beta,
        ROUND(weighted_volatility_pct, 2) as Volatility_Pct,
        ROUND(var_1day_95_usd, 0) as VaR_1Day_95,
        num_positions
    FROM {catalog}.{gold_schema}.portfolio_risk_metrics
    ORDER BY aum_usd DESC
"""))

# COMMAND ----------

print("🎯 Stress Test Impact Heatmap:")
display(spark.sql(f"""
    SELECT
        manager_name,
        scenario_name,
        ROUND(impact_pct, 2) as Impact_Pct,
        ROUND(total_impact_usd, 0) as Impact_USD
    FROM {catalog}.{gold_schema}.stress_test_results
    ORDER BY manager_name, scenario_name
"""))

# COMMAND ----------

print("🏷️ Sector Exposures by Manager:")
display(spark.sql(f"""
    SELECT
        manager_name,
        sector,
        ROUND(sector_weight_pct, 2) as Weight_Pct
    FROM {catalog}.{gold_schema}.sector_exposures
    WHERE sector_weight_pct > 5  -- Show sectors > 5%
    ORDER BY manager_name, sector_weight_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Next Steps
# MAGIC
# MAGIC 1. ✅ Portfolio risk metrics computed (VaR, beta, volatility)
# MAGIC 2. ✅ Stress tests executed across scenarios
# MAGIC 3. ✅ Factor and sector exposures calculated
# MAGIC 4. ⏭️ **Next**: Run `04_agent_workflow.py` to deploy multi-agent AI system

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

