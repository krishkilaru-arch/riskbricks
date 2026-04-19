# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 RiskBricks Complete Forecast Demo
# MAGIC
# MAGIC **Purpose**: Run forecasts for all 20 stocks and generate summary report
# MAGIC
# MAGIC **Output**:
# MAGIC - Forecasts for all 20 stocks
# MAGIC - Performance summary by sector
# MAGIC - Top 10 buy/sell recommendations
# MAGIC - Model consensus analysis

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import pandas as pd

catalog = "riskbricks"
target_date = "2026-02-04"

print("🚀 RiskBricks Complete Forecast Demo")
print("=" * 60)
print(f"📅 Target Date: {target_date}")
print(f"📊 Catalog: {catalog}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Run Forecasts for All 20 Stocks

# COMMAND ----------

# Top 20 stocks
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 
           'JPM', 'V', 'WMT', 'JNJ', 'PG', 'MA', 'HD', 'BAC',
           'XOM', 'CVX', 'KO', 'DIS', 'NFLX']

print(f"🎯 Running forecasts for {len(symbols)} stocks...")
print()

successful = 0
failed = 0
results = []

for i, symbol in enumerate(symbols, 1):
    try:
        print(f"[{i:2d}/{len(symbols)}] Processing {symbol:6s}...", end=" ")
        
        result = dbutils.notebook.run(
            "/Shared/riskbricks/notebooks/agents/02_forecast_agent",
            timeout_seconds=600,
            arguments={
                "symbol": symbol,
                "target_date": target_date,
                "method": "mean",
                "mode": "fast"
            }
        )
        
        if "success" in result.lower():
            print("✅")
            successful += 1
            results.append({"symbol": symbol, "status": "success"})
        else:
            print(f"⚠️  {result}")
            failed += 1
            results.append({"symbol": symbol, "status": "warning"})
    except Exception as e:
        print(f"❌ Error: {str(e)[:50]}")
        failed += 1
        results.append({"symbol": symbol, "status": "failed"})

print()
print("=" * 60)
print(f"✅ Successful: {successful}/{len(symbols)}")
print(f"❌ Failed: {failed}/{len(symbols)}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Load All Forecast Results

# COMMAND ----------

forecasts_df = spark.sql(f"""
    SELECT 
        f.symbol,
        c.company_name,
        c.sector,
        f.method,
        f.expected_price,
        f.lower_1s,
        f.upper_1s,
        f.last_price,
        ROUND((f.expected_price - f.last_price) / f.last_price * 100, 2) as expected_return_pct,
        ROUND((f.upper_1s - f.expected_price) / f.expected_price * 100, 2) as upside_pct,
        ROUND((f.expected_price - f.lower_1s) / f.expected_price * 100, 2) as downside_pct,
        f.ingestion_timestamp
    FROM {catalog}.gold.forecast_daily f
    JOIN {catalog}.gold.company_universe c ON f.symbol = c.symbol
    WHERE f.target_date = '{target_date}'
    ORDER BY f.symbol, f.method
""")

total_forecasts = forecasts_df.count()
print(f"📊 Loaded {total_forecasts} forecasts")
print()

# Show sample
forecasts_df.select(
    "symbol", "company_name", "method", "expected_price", 
    "last_price", "expected_return_pct"
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Consensus Forecast (Average Across Models)

# COMMAND ----------

consensus_df = spark.sql(f"""
    SELECT 
        f.symbol,
        c.company_name,
        c.sector,
        c.beta,
        c.volatility_30d,
        COUNT(DISTINCT f.method) as num_models,
        ROUND(AVG(f.expected_price), 2) as consensus_price,
        ROUND(AVG(f.last_price), 2) as current_price,
        ROUND(AVG((f.expected_price - f.last_price) / f.last_price * 100), 2) as consensus_return_pct,
        ROUND(STDDEV((f.expected_price - f.last_price) / f.last_price * 100), 2) as model_disagreement,
        ROUND(AVG((f.upper_1s - f.expected_price) / f.expected_price * 100), 2) as avg_upside_pct,
        ROUND(AVG((f.expected_price - f.lower_1s) / f.expected_price * 100), 2) as avg_downside_pct
    FROM {catalog}.gold.forecast_daily f
    JOIN {catalog}.gold.company_universe c ON f.symbol = c.symbol
    WHERE f.target_date = '{target_date}'
    GROUP BY f.symbol, c.company_name, c.sector, c.beta, c.volatility_30d
    ORDER BY consensus_return_pct DESC
""")

print("🎯 Consensus Forecasts (Averaged Across All Models):")
print("=" * 100)
consensus_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Top 10 Buy Recommendations

# COMMAND ----------

print("🟢 TOP 10 BUY RECOMMENDATIONS")
print("=" * 100)

buy_recs = consensus_df.filter(F.col("consensus_return_pct") > 0).orderBy(F.col("consensus_return_pct").desc()).limit(10)
buy_recs.select(
    "symbol", "company_name", "sector", 
    "current_price", "consensus_price", "consensus_return_pct",
    "model_disagreement", "avg_upside_pct"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Top 10 Sell Recommendations

# COMMAND ----------

print("🔴 TOP 10 SELL RECOMMENDATIONS")
print("=" * 100)

sell_recs = consensus_df.filter(F.col("consensus_return_pct") < 0).orderBy(F.col("consensus_return_pct").asc()).limit(10)
sell_recs.select(
    "symbol", "company_name", "sector",
    "current_price", "consensus_price", "consensus_return_pct",
    "model_disagreement", "avg_downside_pct"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Sector Performance Summary

# COMMAND ----------

sector_summary = spark.sql(f"""
    SELECT 
        c.sector,
        COUNT(DISTINCT f.symbol) as num_stocks,
        ROUND(AVG((f.expected_price - f.last_price) / f.last_price * 100), 2) as avg_expected_return_pct,
        ROUND(MIN((f.expected_price - f.last_price) / f.last_price * 100), 2) as min_return_pct,
        ROUND(MAX((f.expected_price - f.last_price) / f.last_price * 100), 2) as max_return_pct,
        ROUND(AVG(c.beta), 2) as avg_beta,
        ROUND(AVG(c.volatility_30d), 2) as avg_volatility
    FROM {catalog}.gold.forecast_daily f
    JOIN {catalog}.gold.company_universe c ON f.symbol = c.symbol
    WHERE f.target_date = '{target_date}'
    GROUP BY c.sector
    ORDER BY avg_expected_return_pct DESC
""")

print("📊 SECTOR PERFORMANCE OUTLOOK")
print("=" * 100)
sector_summary.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Model Performance Analysis

# COMMAND ----------

model_performance = spark.sql(f"""
    SELECT 
        f.method,
        COUNT(*) as num_forecasts,
        ROUND(AVG((f.expected_price - f.last_price) / f.last_price * 100), 2) as avg_return_pct,
        ROUND(STDDEV((f.expected_price - f.last_price) / f.last_price * 100), 2) as std_dev,
        ROUND(MIN((f.expected_price - f.last_price) / f.last_price * 100), 2) as min_return,
        ROUND(MAX((f.expected_price - f.last_price) / f.last_price * 100), 2) as max_return
    FROM {catalog}.gold.forecast_daily f
    WHERE f.target_date = '{target_date}'
    GROUP BY f.method
    ORDER BY avg_return_pct DESC
""")

print("🤖 MODEL PERFORMANCE COMPARISON")
print("=" * 100)
model_performance.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ High Conviction Plays (Low Model Disagreement)

# COMMAND ----------

high_conviction = consensus_df.filter(
    (F.col("model_disagreement") < 2.0) &  # Models agree
    (F.abs(F.col("consensus_return_pct")) > 1.0)  # Significant move expected
).orderBy(F.abs(F.col("consensus_return_pct")).desc())

print("💎 HIGH CONVICTION PLAYS (Models Agree + Significant Move Expected)")
print("=" * 100)
high_conviction.select(
    "symbol", "company_name", "sector",
    "consensus_return_pct", "model_disagreement",
    "num_models", "beta", "volatility_30d"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Risk-Adjusted Opportunities

# COMMAND ----------

# Calculate Sharpe-like ratio: return / volatility
risk_adjusted = consensus_df.withColumn(
    "risk_adjusted_return",
    F.round(F.col("consensus_return_pct") / F.col("volatility_30d"), 3)
).orderBy(F.col("risk_adjusted_return").desc())

print("⚖️ RISK-ADJUSTED OPPORTUNITIES (Return / Volatility)")
print("=" * 100)
risk_adjusted.select(
    "symbol", "company_name", "sector",
    "consensus_return_pct", "volatility_30d", "beta",
    "risk_adjusted_return"
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔟 Executive Summary

# COMMAND ----------

# Calculate summary stats
summary_stats = consensus_df.agg(
    F.count("symbol").alias("total_stocks"),
    F.sum(F.when(F.col("consensus_return_pct") > 0, 1).otherwise(0)).alias("bullish_count"),
    F.sum(F.when(F.col("consensus_return_pct") < 0, 1).otherwise(0)).alias("bearish_count"),
    F.sum(F.when(F.col("consensus_return_pct") == 0, 1).otherwise(0)).alias("neutral_count"),
    F.round(F.avg("consensus_return_pct"), 2).alias("avg_expected_return"),
    F.round(F.avg("model_disagreement"), 2).alias("avg_model_disagreement"),
    F.round(F.avg("beta"), 2).alias("avg_beta"),
    F.round(F.avg("volatility_30d"), 2).alias("avg_volatility")
).collect()[0]

# Get best and worst
best_stock = consensus_df.orderBy(F.col("consensus_return_pct").desc()).first()
worst_stock = consensus_df.orderBy(F.col("consensus_return_pct").asc()).first()
most_volatile = consensus_df.orderBy(F.col("model_disagreement").desc()).first()
safest_bet = consensus_df.filter(F.col("consensus_return_pct") > 0).orderBy(F.col("model_disagreement").asc()).first()

print("=" * 80)
print("📈 RISKBRICKS FORECAST SUMMARY")
print("=" * 80)
print(f"""
📊 Portfolio Universe: {summary_stats.total_stocks} stocks

Market Sentiment:
  🟢 Bullish:  {summary_stats.bullish_count} stocks ({summary_stats.bullish_count/summary_stats.total_stocks*100:.1f}%)
  🔴 Bearish:  {summary_stats.bearish_count} stocks ({summary_stats.bearish_count/summary_stats.total_stocks*100:.1f}%)
  ⚪ Neutral:  {summary_stats.neutral_count} stocks ({summary_stats.neutral_count/summary_stats.total_stocks*100:.1f}%)

Average Metrics:
  • Expected Return: {summary_stats.avg_expected_return:+.2f}%
  • Model Agreement: {summary_stats.avg_model_disagreement:.2f}% disagreement
  • Beta: {summary_stats.avg_beta:.2f}
  • Volatility: {summary_stats.avg_volatility:.2f}%

Top Picks:
  🏆 Best Opportunity: {best_stock.symbol} ({best_stock.company_name})
     → {best_stock.consensus_return_pct:+.2f}% expected return
     → Sector: {best_stock.sector}
     → Model disagreement: {best_stock.model_disagreement:.2f}%
  
  💎 Safest Bullish Bet: {safest_bet.symbol if safest_bet else 'N/A'} ({safest_bet.company_name if safest_bet else 'N/A'})
     → {safest_bet.consensus_return_pct if safest_bet else 0:+.2f}% expected return
     → Model disagreement: {safest_bet.model_disagreement if safest_bet else 0:.2f}%

Risks to Watch:
  ⚠️ Weakest Stock: {worst_stock.symbol} ({worst_stock.company_name})
     → {worst_stock.consensus_return_pct:+.2f}% expected return
     → Sector: {worst_stock.sector}
  
  🎲 Most Uncertain: {most_volatile.symbol} ({most_volatile.company_name})
     → Model disagreement: {most_volatile.model_disagreement:.2f}%

Target Date: {target_date}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Export Results

# COMMAND ----------

# Save consensus forecasts to table
consensus_df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.forecast_consensus")

print(f"✅ Saved consensus forecasts to {catalog}.gold.forecast_consensus")
print()
print("📊 Available for downstream analysis:")
print("   - Databricks Apps")
print("   - Mosaic AI Agents")
print("   - BI Tools (Tableau, Power BI)")
print("   - API endpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")
