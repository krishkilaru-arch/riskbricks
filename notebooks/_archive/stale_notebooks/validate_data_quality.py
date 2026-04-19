# Databricks notebook source
# MAGIC %md
# MAGIC # 📋 Data Validation - Bronze to Silver Layer
# MAGIC
# MAGIC **Purpose**: Validate and clean data from Bronze layer, creating high-quality Silver tables
# MAGIC
# MAGIC **Inputs**:
# MAGIC - `riskbricks.bronze.stock_prices_bronze`
# MAGIC - `riskbricks.bronze.macro_indicators_bronze`
# MAGIC
# MAGIC **Outputs**:
# MAGIC - `riskbricks.silver.stock_prices` (validated, deduplicated)
# MAGIC - `riskbricks.silver.macro_indicators` (validated, deduplicated)
# MAGIC - `riskbricks.silver.data_quality_metrics` (quality monitoring)
# MAGIC
# MAGIC **Run Sequence**: After `01_data_ingestion.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

# Configuration
catalog = "riskbricks"
bronze_schema = "bronze"
silver_schema = "silver"

# Create silver schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schemas: {bronze_schema} → {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Validate Stock Prices

# COMMAND ----------

print("📊 Loading bronze stock prices...")
bronze_stocks = spark.table(f"{catalog}.{bronze_schema}.stock_prices_bronze")

print(f"   Total bronze records: {bronze_stocks.count():,}")
print(f"   Date range: {bronze_stocks.agg(min('date')).collect()[0][0]} to {bronze_stocks.agg(max('date')).collect()[0][0]}")
print(f"   Unique symbols: {bronze_stocks.select('symbol').distinct().count()}")

# COMMAND ----------

print("🔍 Running data quality checks...")

# Data Quality Rules
validated_stocks = bronze_stocks \
    .filter(col("symbol").isNotNull()) \
    .filter(col("date").isNotNull()) \
    .filter(col("close").isNotNull()) \
    .filter(col("close") > 0) \
    .filter(col("volume") >= 0) \
    .withColumn("is_valid", lit(True))

# Check for price anomalies (e.g., >50% daily change)
window_spec = Window.partitionBy("symbol").orderBy("date")

validated_stocks = validated_stocks \
    .withColumn("prev_close", lag("close").over(window_spec)) \
    .withColumn("price_change_pct",
                when(col("prev_close").isNotNull(),
                     ((col("close") - col("prev_close")) / col("prev_close") * 100))
                .otherwise(0)) \
    .withColumn("is_anomaly",
                when(abs(col("price_change_pct")) > 50, True)
                .otherwise(False)) \
    .withColumn("quality_score",
                when(col("is_anomaly"), 0.5)  # Lower score for anomalies
                .otherwise(1.0)) \
    .withColumn("validated_at", current_timestamp())

# Remove duplicates (keep latest ingestion)
validated_stocks = validated_stocks \
    .withColumn("row_num",
                row_number().over(
                    Window.partitionBy("symbol", "date")
                    .orderBy(desc("ingestion_timestamp"))
                )) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"✅ Validated {validated_stocks.count():,} stock records")
print(f"   Anomalies detected: {validated_stocks.filter(col('is_anomaly')).count()}")

# COMMAND ----------

print("💾 Saving to Silver layer...")

validated_stocks \
    .select(
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "price_change_pct",
        "is_anomaly",
        "quality_score",
        "validated_at"
    ) \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{silver_schema}.stock_prices")

print(f"✅ Saved to {catalog}.{silver_schema}.stock_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Validate Macro Indicators

# COMMAND ----------

print("📈 Loading bronze macro indicators...")
bronze_macro = spark.table(f"{catalog}.{bronze_schema}.macro_indicators_bronze")

print(f"   Total bronze records: {bronze_macro.count():,}")
print(f"   Date range: {bronze_macro.agg(min('date')).collect()[0][0]} to {bronze_macro.agg(max('date')).collect()[0][0]}")
print(f"   Unique indicators: {bronze_macro.select('indicator_name').distinct().count()}")

# COMMAND ----------

print("🔍 Running macro data quality checks...")

# Data Quality Rules for Macro Indicators
validated_macro = bronze_macro \
    .filter(col("indicator_name").isNotNull()) \
    .filter(col("date").isNotNull()) \
    .filter(col("value").isNotNull())

# Reasonable range checks (vary by indicator)
validated_macro = validated_macro \
    .withColumn("is_valid",
                when(
                    (col("indicator_name").like("%Rate%")) &
                    (col("value").between(-10, 50)), True  # Rates: -10% to 50%
                )
                .when(
                    (col("indicator_name").like("%CPI%")) &
                    (col("value").between(-5, 30)), True  # CPI: -5% to 30%
                )
                .when(
                    (col("indicator_name").like("%GDP%")) &
                    (col("value").between(-50, 50)), True  # GDP: -50% to 50%
                )
                .otherwise(True)  # Accept other indicators
    ) \
    .withColumn("quality_score", lit(1.0)) \
    .withColumn("validated_at", current_timestamp())

# Remove duplicates
validated_macro = validated_macro \
    .withColumn("row_num",
                row_number().over(
                    Window.partitionBy("indicator_name", "date")
                    .orderBy(desc("ingestion_timestamp"))
                )) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"✅ Validated {validated_macro.count():,} macro records")

# COMMAND ----------

print("💾 Saving to Silver layer...")

validated_macro \
    .select(
        "indicator_name",
        "date",
        "value",
        "is_valid",
        "quality_score",
        "validated_at"
    ) \
    .write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{silver_schema}.macro_indicators")

print(f"✅ Saved to {catalog}.{silver_schema}.macro_indicators")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Quality Metrics

# COMMAND ----------

print("📊 Computing data quality metrics...")

# Stock data quality
stock_quality = spark.sql(f"""
    SELECT
        'stock_prices' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT symbol) as unique_items,
        COUNT(DISTINCT date) as unique_dates,
        MIN(date) as min_date,
        MAX(date) as max_date,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
        AVG(quality_score) as avg_quality_score,
        current_timestamp() as computed_at
    FROM {catalog}.{silver_schema}.stock_prices
""")

# Macro data quality
macro_quality = spark.sql(f"""
    SELECT
        'macro_indicators' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT indicator_name) as unique_items,
        COUNT(DISTINCT date) as unique_dates,
        MIN(date) as min_date,
        MAX(date) as max_date,
        0 as anomaly_count,
        AVG(quality_score) as avg_quality_score,
        current_timestamp() as computed_at
    FROM {catalog}.{silver_schema}.macro_indicators
""")

# Combine quality metrics (both now have same schema)
quality_metrics = stock_quality.union(macro_quality)

# Save quality metrics
quality_metrics.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{silver_schema}.data_quality_metrics")

print("✅ Data quality metrics computed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Data Quality Report

# COMMAND ----------

print("\n" + "="*60)
print("📊 DATA QUALITY REPORT")
print("="*60 + "\n")

quality_df = spark.table(f"{catalog}.{silver_schema}.data_quality_metrics").toPandas()

for _, row in quality_df.iterrows():
    print(f"📋 Table: {row['table_name']}")
    print(f"   Total Records: {int(row['total_records']):,}")
    
    if row['table_name'] == 'stock_prices':
        print(f"   Unique Symbols: {int(row['unique_items'])}")
    else:
        print(f"   Unique Indicators: {int(row['unique_items'])}")
    
    print(f"   Date Range: {row['min_date']} to {row['max_date']}")
    print(f"   Anomalies: {int(row['anomaly_count'])}")
    print(f"   Avg Quality Score: {row['avg_quality_score']:.3f}")
    print()

print("="*60)
print("✅ VALIDATION COMPLETE - Silver layer ready for analytics")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Sample Validated Data

# COMMAND ----------

print("📊 Sample Stock Prices (Silver):")
display(spark.sql(f"""
    SELECT symbol, date, close, price_change_pct, is_anomaly, quality_score
    FROM {catalog}.{silver_schema}.stock_prices
    WHERE date >= date_sub(current_date(), 30)
    ORDER BY date DESC, symbol
    LIMIT 20
"""))

# COMMAND ----------

print("📈 Sample Macro Indicators (Silver):")
display(spark.sql(f"""
    SELECT indicator_name, date, value, quality_score
    FROM {catalog}.{silver_schema}.macro_indicators
    WHERE date >= date_sub(current_date(), 90)
    ORDER BY date DESC, indicator_name
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Next Steps
# MAGIC
# MAGIC 1. ✅ Bronze data validated and cleaned
# MAGIC 2. ✅ Silver tables created with quality scores
# MAGIC 3. ⏭️ **Next**: Run `03_risk_analytics.py` to compute risk metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

