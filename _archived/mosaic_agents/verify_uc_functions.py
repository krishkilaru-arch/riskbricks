# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ Verify UC Functions for Agent Deployment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all UC functions in riskbricks.tools
# MAGIC %sql
# MAGIC -- List all UC functions in riskbricks.tools
# MAGIC USE CATALOG riskbricks; SHOW USER FUNCTIONS IN riskbricks.tools;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Quick Smoke Tests

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 1: Company Info
# MAGIC SELECT riskbricks.tools.get_company_info('AAPL') as result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 2: Latest Forecast
# MAGIC SELECT riskbricks.tools.get_latest_forecast('AAPL', '2026-02-03') as result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 3: Risk Metrics
# MAGIC SELECT riskbricks.tools.get_risk_metrics('NVDA', '2026-02-03') as result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 4: Decision Signal
# MAGIC SELECT riskbricks.tools.get_decision_signal('MSFT', '2026-02-03') as result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 5: Top Opportunities (Portfolio-level)
# MAGIC SELECT riskbricks.tools.get_top_opportunities('2026-02-03', 5) as result;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Expected Results
# MAGIC
# MAGIC All queries should return JSON with data:
# MAGIC - ✅ No errors
# MAGIC - ✅ JSON responses
# MAGIC - ✅ Actual data (not empty arrays)
# MAGIC
# MAGIC If any test fails, run diagnostic queries in the UC functions notebook.

# COMMAND ----------

dbutils.notebook.exit("success")
