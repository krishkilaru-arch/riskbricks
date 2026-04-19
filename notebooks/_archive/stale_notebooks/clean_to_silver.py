# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 Clean RAG Corpus - Bronze to Silver
# MAGIC
# MAGIC **Purpose**: Clean, validate, and deduplicate the raw RAG corpus
# MAGIC
# MAGIC **Medallion Architecture**:
# MAGIC ```
# MAGIC Bronze (Raw)     →    Silver (Cleaned)    →    Gold (Aggregated)
# MAGIC rag_corpus            rag_documents            rag_analytics
# MAGIC ```
# MAGIC
# MAGIC **Cleaning Steps**:
# MAGIC 1. Remove duplicates
# MAGIC 2. Validate required fields
# MAGIC 3. Standardize text (trim, clean HTML)
# MAGIC 4. Add quality scores
# MAGIC 5. Create text for embedding

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import re

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")

bronze_table = f"{catalog}.bronze.rag_corpus"
silver_table = f"{catalog}.silver.rag_documents"

print(f"📥 Source: {bronze_table}")
print(f"📤 Target: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load Bronze Data

# COMMAND ----------

# Load raw data
bronze_df = spark.table(bronze_table)

print(f"📊 Bronze layer statistics:")
print(f"   Total records: {bronze_df.count():,}")
print(f"   Columns: {len(bronze_df.columns)}")

# Show document type distribution
print("\n📁 Document types:")
bronze_df.groupBy("doc_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Step 1: Remove Duplicates

# COMMAND ----------

# Deduplicate by doc_id (should already be unique, but verify)
before_count = bronze_df.count()
deduped_df = bronze_df.dropDuplicates(["doc_id"])
after_count = deduped_df.count()

print(f"🔄 Deduplication:")
print(f"   Before: {before_count:,}")
print(f"   After: {after_count:,}")
print(f"   Removed: {before_count - after_count:,} duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Step 2: Validate Required Fields

# COMMAND ----------

# Filter out records with missing critical fields
validated_df = deduped_df.filter(
    F.col("symbol").isNotNull() &
    F.col("title").isNotNull() &
    (F.length(F.col("title")) > 5) &  # Title must be meaningful
    F.col("doc_type").isNotNull()
)

validation_removed = after_count - validated_df.count()
print(f"✅ Validation:")
print(f"   Valid records: {validated_df.count():,}")
print(f"   Invalid removed: {validation_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Step 3: Clean and Standardize Text

# COMMAND ----------

# UDF to clean text
@F.udf(returnType=StringType())
def clean_text(text):
    if not text:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?;:\-\'\"()]', '', text)
    
    # Trim
    text = text.strip()
    
    return text

# Apply cleaning
cleaned_df = validated_df \
    .withColumn("title_clean", clean_text(F.col("title"))) \
    .withColumn("content_clean", clean_text(F.col("content"))) \
    .withColumn("title", F.col("title_clean")) \
    .withColumn("content", F.col("content_clean")) \
    .drop("title_clean", "content_clean")

print("✅ Text cleaning applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Step 4: Add Quality Scores

# COMMAND ----------

# Calculate quality metrics
silver_df = cleaned_df \
    .withColumn("title_length", F.length(F.col("title"))) \
    .withColumn("content_length", F.length(F.col("content"))) \
    .withColumn("has_url", F.when(F.col("url").isNotNull() & (F.length(F.col("url")) > 10), 1).otherwise(0)) \
    .withColumn("has_date", F.when(F.col("published_date").isNotNull(), 1).otherwise(0)) \
    .withColumn(
        "quality_score",
        (
            F.when(F.col("title_length") > 20, 0.3).otherwise(0.1) +
            F.when(F.col("content_length") > 100, 0.3).otherwise(0.1) +
            F.when(F.col("content_length") > 500, 0.2).otherwise(0.0) +
            F.col("has_url") * 0.1 +
            F.col("has_date") * 0.1
        )
    ) \
    .withColumn("cleaned_at", F.current_timestamp())

print("✅ Quality scores calculated")

# Show quality distribution
print("\n📊 Quality Score Distribution:")
silver_df.groupBy(
    F.when(F.col("quality_score") >= 0.8, "High (≥0.8)")
     .when(F.col("quality_score") >= 0.5, "Medium (0.5-0.8)")
     .otherwise("Low (<0.5)").alias("quality_tier")
).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Step 5: Create Text for Embedding

# COMMAND ----------

# Create optimized text for embedding
silver_final = silver_df \
    .withColumn(
        "text_for_embedding",
        F.concat_ws(
            ". ",
            F.concat(F.lit("Document Type: "), F.col("doc_type")),
            F.concat(F.lit("Company: "), F.col("company_name"), F.lit(" ("), F.col("symbol"), F.lit(")")),
            F.concat(F.lit("Sector: "), F.coalesce(F.col("sector"), F.lit("Unknown"))),
            F.concat(F.lit("Title: "), F.col("title")),
            F.concat(F.lit("Content: "), F.substring(F.col("content"), 1, 2000))
        )
    )

# Select final columns
silver_output = silver_final.select(
    "doc_id",
    "symbol",
    "company_name",
    "sector",
    "doc_type",
    "title",
    "content",
    "source",
    "url",
    "published_date",
    "ingestion_timestamp",
    "title_length",
    "content_length",
    "quality_score",
    "cleaned_at",
    "text_for_embedding"
)

print(f"✅ Silver dataset ready: {silver_output.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Silver Layer

# COMMAND ----------

# Save to Silver
silver_output.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

spark.sql(f"COMMENT ON TABLE {silver_table} IS 'Cleaned RAG documents - deduplicated, validated, with quality scores'")

# Verify
record_count = spark.sql(f"SELECT COUNT(*) FROM {silver_table}").collect()[0][0]
print(f"✅ Saved {record_count:,} records to {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Silver Layer Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Document quality by type
# MAGIC SELECT 
# MAGIC   doc_type,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   ROUND(AVG(quality_score), 2) as avg_quality,
# MAGIC   ROUND(AVG(content_length), 0) as avg_content_length,
# MAGIC   COUNT(DISTINCT symbol) as unique_stocks
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC GROUP BY doc_type
# MAGIC ORDER BY doc_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High quality documents
# MAGIC SELECT 
# MAGIC   doc_type,
# MAGIC   symbol,
# MAGIC   title,
# MAGIC   quality_score,
# MAGIC   source
# MAGIC FROM riskbricks.silver.rag_documents
# MAGIC WHERE quality_score >= 0.8
# MAGIC ORDER BY quality_score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

print(f"""
================================================================================
✅ SILVER LAYER COMPLETE!
================================================================================

📊 Cleaning Summary:
   - Duplicates removed
   - Invalid records filtered
   - Text cleaned (HTML, whitespace)
   - Quality scores calculated
   - Embedding text created

📈 Statistics:
   - Total documents: {record_count:,}
   - Table: {silver_table}

🔄 Next Step:
   Run notebooks/03_gold/rag/create_gold_analytics.py for aggregated views

================================================================================
""")

dbutils.notebook.exit("success")
