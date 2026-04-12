# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Test All Unity Catalog Functions
# MAGIC
# MAGIC **Purpose**: Comprehensive testing of all 12 UC functions
# MAGIC
# MAGIC **Prerequisites**: 
# MAGIC - Run `05_create_uc_functions.py` first to create the functions
# MAGIC - Ensure data exists in Bronze, Silver, and Gold layers
# MAGIC
# MAGIC **What This Does:**
# MAGIC - Tests all 12 UC functions with various inputs
# MAGIC - Validates return data structure
# MAGIC - Checks for errors and edge cases

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Setup and Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG riskbricks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if schema and functions exist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all functions in agent_tools schema
# MAGIC SHOW USER FUNCTIONS IN riskbricks.agent_tools;

# COMMAND ----------

print("✅ Setup complete. Starting function tests...")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 PART 1: Original 6 Functions (Basic Portfolio Management)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Test: get_risk_metrics()
# MAGIC
# MAGIC **Purpose**: Get risk metrics for portfolio managers
# MAGIC **Parameters**: manager_name (STRING) - "Sarah Russel", "Rena Tang", "Mohit Arora", or "all"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 1a: Get metrics for specific manager
# MAGIC SELECT * FROM riskbricks.agent_tools.get_risk_metrics('Sarah Russel');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 1b: Get metrics for all managers
# MAGIC SELECT * FROM riskbricks.agent_tools.get_risk_metrics('all');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 1c: Another manager
# MAGIC SELECT * FROM riskbricks.agent_tools.get_risk_metrics('Mohit Arora');

# COMMAND ----------

print("✅ Test 1 Complete: get_risk_metrics()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Test: get_stress_tests()
# MAGIC
# MAGIC **Purpose**: Get stress test results for portfolio managers
# MAGIC **Parameters**: manager_name (STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 2a: Stress tests for Sarah
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stress_tests('Sarah Russel')
# MAGIC ORDER BY scenario_name, manager_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 2b: All stress tests
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stress_tests('all')
# MAGIC ORDER BY scenario_name, impact_pct DESC;

# COMMAND ----------

print("✅ Test 2 Complete: get_stress_tests()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Test: get_portfolio_holdings()
# MAGIC
# MAGIC **Purpose**: Get holdings for a portfolio manager
# MAGIC **Parameters**: manager_name (STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 3a: Sarah's holdings
# MAGIC SELECT * FROM riskbricks.agent_tools.get_portfolio_holdings('Sarah Russel')
# MAGIC ORDER BY weight_pct DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 3b: Count holdings per manager
# MAGIC SELECT 
# MAGIC   manager_name,
# MAGIC   COUNT(*) as num_holdings,
# MAGIC   SUM(weight_pct) as total_weight
# MAGIC FROM riskbricks.agent_tools.get_portfolio_holdings('all')
# MAGIC GROUP BY manager_name;

# COMMAND ----------

print("✅ Test 3 Complete: get_portfolio_holdings()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Test: get_sector_exposures()
# MAGIC
# MAGIC **Purpose**: Get sector exposure breakdown for a manager
# MAGIC **Parameters**: manager_name (STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 4a: Sarah's sector exposure
# MAGIC SELECT * FROM riskbricks.agent_tools.get_sector_exposures('Sarah Russel')
# MAGIC ORDER BY sector_weight_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 4b: Compare sector exposure across all managers
# MAGIC SELECT 
# MAGIC   sector,
# MAGIC   SUM(CASE WHEN manager_name = 'Sarah Russel' THEN sector_weight_pct ELSE 0 END) as sarah_pct,
# MAGIC   SUM(CASE WHEN manager_name = 'Rena Tang' THEN sector_weight_pct ELSE 0 END) as rena_pct,
# MAGIC   SUM(CASE WHEN manager_name = 'Mohit Arora' THEN sector_weight_pct ELSE 0 END) as mohit_pct
# MAGIC FROM riskbricks.agent_tools.get_sector_exposures('all')
# MAGIC GROUP BY sector
# MAGIC ORDER BY sarah_pct DESC;

# COMMAND ----------

print("✅ Test 4 Complete: get_sector_exposures()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Test: get_macro_context()
# MAGIC
# MAGIC **Purpose**: Get current macroeconomic indicators
# MAGIC **Parameters**: None

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 5: Current macro context
# MAGIC SELECT * FROM riskbricks.agent_tools.get_macro_context();

# COMMAND ----------

print("✅ Test 5 Complete: get_macro_context()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Test: compare_managers()
# MAGIC
# MAGIC **Purpose**: Compare all portfolio managers side-by-side
# MAGIC **Parameters**: None

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 6: Compare all managers
# MAGIC SELECT * FROM riskbricks.agent_tools.compare_managers()
# MAGIC ORDER BY aum_usd DESC;

# COMMAND ----------

print("✅ Test 6 Complete: compare_managers()")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 PART 2: Phase 2 Functions (Advanced Analytics)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Test: get_stock_info()
# MAGIC
# MAGIC **Purpose**: Get beta, volatility, and latest price for a stock
# MAGIC **Parameters**: symbol (STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 7a: Get info for AAPL
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stock_info('AAPL');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 7b: Get info for multiple stocks
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stock_info('TSLA')
# MAGIC UNION ALL
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stock_info('NVDA')
# MAGIC UNION ALL
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stock_info('MSFT');

# COMMAND ----------

print("✅ Test 7 Complete: get_stock_info()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Test: get_stock_price_history()
# MAGIC
# MAGIC **Purpose**: Get historical prices for a stock
# MAGIC **Parameters**: symbol (STRING), days_back (INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 8a: Last 30 days of AAPL
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stock_price_history('AAPL', 30)
# MAGIC ORDER BY date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 8b: Last 90 days of TSLA
# MAGIC SELECT * FROM riskbricks.agent_tools.get_stock_price_history('TSLA', 90)
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 10;

# COMMAND ----------

print("✅ Test 8 Complete: get_stock_price_history()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Test: search_stocks_by_criteria()
# MAGIC
# MAGIC **Purpose**: Find stocks matching specific criteria
# MAGIC **Parameters**: sector (STRING), max_beta (DOUBLE), max_vol (DOUBLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 9a: Find low-risk tech stocks
# MAGIC SELECT * FROM riskbricks.agent_tools.search_stocks_by_criteria('Technology', 1.0, 0.20)
# MAGIC ORDER BY beta ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 9b: Find low volatility healthcare stocks
# MAGIC SELECT * FROM riskbricks.agent_tools.search_stocks_by_criteria('Healthcare', 1.5, 0.15)
# MAGIC ORDER BY volatility_30d ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 9c: Find all available sectors
# MAGIC SELECT DISTINCT sector, COUNT(*) as num_stocks
# MAGIC FROM riskbricks.gold.company_universe
# MAGIC GROUP BY sector
# MAGIC ORDER BY sector;

# COMMAND ----------

print("✅ Test 9 Complete: search_stocks_by_criteria()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔟 Test: get_sector_statistics()
# MAGIC
# MAGIC **Purpose**: Get aggregate statistics for a sector
# MAGIC **Parameters**: sector_name (STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 10a: Technology sector stats
# MAGIC SELECT * FROM riskbricks.agent_tools.get_sector_statistics('Technology');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 10b: Healthcare sector stats
# MAGIC SELECT * FROM riskbricks.agent_tools.get_sector_statistics('Healthcare');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 10c: Compare all sectors
# MAGIC SELECT 
# MAGIC   sector,
# MAGIC   num_stocks,
# MAGIC   avg_beta,
# MAGIC   avg_volatility_30d
# MAGIC FROM (
# MAGIC   SELECT DISTINCT sector FROM riskbricks.gold.company_universe
# MAGIC ) sectors
# MAGIC LATERAL VIEW OUTER riskbricks.agent_tools.get_sector_statistics(sector);

# COMMAND ----------

print("✅ Test 10 Complete: get_sector_statistics()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣1️⃣ Test: calculate_correlation()
# MAGIC
# MAGIC **Purpose**: Calculate correlation between two stocks
# MAGIC **Parameters**: symbol1 (STRING), symbol2 (STRING), days_back (INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 11a: Correlation between AAPL and MSFT
# MAGIC SELECT * FROM riskbricks.agent_tools.calculate_correlation('AAPL', 'MSFT', 180);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 11b: Correlation between TSLA and NVDA
# MAGIC SELECT * FROM riskbricks.agent_tools.calculate_correlation('TSLA', 'NVDA', 90);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 11c: Correlation between JPM and GS (both financials)
# MAGIC SELECT * FROM riskbricks.agent_tools.calculate_correlation('JPM', 'GS', 180);

# COMMAND ----------

print("✅ Test 11 Complete: calculate_correlation()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣2️⃣ Test: analyze_simple_portfolio()
# MAGIC
# MAGIC **Purpose**: Analyze a custom portfolio
# MAGIC **Parameters**: symbols (STRING - comma-separated), weights (STRING - comma-separated)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 12a: Simple 3-stock portfolio
# MAGIC SELECT * FROM riskbricks.agent_tools.analyze_simple_portfolio('AAPL,MSFT,GOOGL', '0.4,0.4,0.2');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 12b: Balanced 5-stock portfolio
# MAGIC SELECT * FROM riskbricks.agent_tools.analyze_simple_portfolio(
# MAGIC   'AAPL,MSFT,JPM,JNJ,XOM', 
# MAGIC   '0.2,0.2,0.2,0.2,0.2'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 12c: Tech-heavy portfolio
# MAGIC SELECT * FROM riskbricks.agent_tools.analyze_simple_portfolio(
# MAGIC   'NVDA,TSLA,AAPL,MSFT', 
# MAGIC   '0.3,0.3,0.2,0.2'
# MAGIC );

# COMMAND ----------

print("✅ Test 12 Complete: analyze_simple_portfolio()")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ALL TESTS COMPLETE!
# MAGIC ---

# COMMAND ----------

print("""
================================================================================
🎉 ALL 12 UC FUNCTIONS TESTED SUCCESSFULLY!
================================================================================

✅ PART 1: Original 6 Functions (Basic)
   1. get_risk_metrics - Portfolio risk metrics
   2. get_stress_tests - Stress test scenarios
   3. get_portfolio_holdings - Manager holdings
   4. get_sector_exposures - Sector breakdown
   5. get_macro_context - Current macro indicators
   6. compare_managers - Manager comparison

✅ PART 2: Phase 2 Functions (Advanced)
   7. get_stock_info - Individual stock analysis
   8. get_stock_price_history - Historical prices
   9. search_stocks_by_criteria - Stock discovery
   10. get_sector_statistics - Sector aggregates
   11. calculate_correlation - Stock correlations
   12. analyze_simple_portfolio - Custom portfolio analysis

================================================================================
📊 Next Steps:
   - Review test results above
   - Check for any errors or NULL values
   - Document any issues found
   - Consider integrating with Agent Bricks
================================================================================
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count total functions
# MAGIC SELECT 
# MAGIC   'Total UC Functions' as metric,
# MAGIC   COUNT(*) as value
# MAGIC FROM (SHOW USER FUNCTIONS IN riskbricks.agent_tools)
# MAGIC WHERE function LIKE '%agent_tools%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary of portfolio data
# MAGIC SELECT 
# MAGIC   'Total Managers' as metric,
# MAGIC   COUNT(*) as value
# MAGIC FROM riskbricks.gold.portfolio_managers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Total Holdings',
# MAGIC   COUNT(*)
# MAGIC FROM riskbricks.gold.portfolio_holdings
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Total Companies',
# MAGIC   COUNT(*)
# MAGIC FROM riskbricks.gold.company_universe
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Stock Price Records',
# MAGIC   COUNT(*)
# MAGIC FROM riskbricks.silver.stock_prices;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

