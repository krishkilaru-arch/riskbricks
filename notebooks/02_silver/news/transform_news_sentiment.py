# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 News Sentiment Analysis with Llama 3.3 70B
# MAGIC
# MAGIC **Purpose**: Analyze news articles for sentiment and portfolio impact
# MAGIC
# MAGIC **LLM**: meta-llama/Llama-3.3-70B-Instruct (Databricks Foundation Models)
# MAGIC
# MAGIC **Input**: `riskbricks.bronze.news_raw`
# MAGIC **Output**: `riskbricks.silver.news_sentiment`
# MAGIC
# MAGIC **Run Frequency**: Every 2-4 hours (after news ingestion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json
import requests

# Database setup
catalog = "riskbricks"

spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

print(f"✅ Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Get Databricks Token for Foundation Models API

# COMMAND ----------

# Get workspace URL and token
try:
    db_token = dbutils.secrets.get(scope="riskbricks", key="databricks-token")
    print("✅ Using token from secrets")
except:
    # For testing - use notebook context token
    db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    print("⚠️  Using notebook context token")

# Get workspace URL
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
print(f"✅ Workspace: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Sentiment Analysis with Llama 3.3 70B

# COMMAND ----------

def analyze_sentiment_with_llm(title, description, url):
    """
    Analyze news sentiment using Llama 3.3 70B via Databricks Foundation Models API
    """
    
    # Combine title and description
    text = f"{title}\n\n{description}" if description else title
    
    # Construct prompt
    prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are a financial analyst specializing in portfolio risk management. Analyze news articles for their impact on stock portfolios.

<|eot_id|><|start_header_id|>user<|end_header_id|>

Analyze this financial news article:

ARTICLE:
{text}

Provide analysis in this EXACT JSON format (no extra text):
{{
  "sentiment": "bullish|neutral|bearish",
  "sentiment_score": <number between -1.0 (very bearish) and 1.0 (very bullish)>,
  "risk_level": "low|medium|high",
  "affected_sectors": [<list of sector names>],
  "affected_stocks": [<list of stock tickers if mentioned>],
  "portfolio_impact": "<one sentence summary>",
  "key_topics": [<list of 2-3 key topics like 'interest_rates', 'earnings', 'geopolitical'>],
  "confidence": <number between 0.0 and 1.0>
}}

<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
    
    # Call Databricks Foundation Model API
    api_url = f"https://{workspace_url}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations"
    
    headers = {
        "Authorization": f"Bearer {db_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [
            {"role": "system", "content": "You are a financial analyst. Always respond with valid JSON only."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,  # Low temperature for more consistent output
        "max_tokens": 500
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            content = result.get('choices', [{}])[0].get('message', {}).get('content', '{}')
            
            # Try to parse JSON from response
            try:
                # Extract JSON from response (sometimes LLM adds extra text)
                if '{' in content and '}' in content:
                    json_start = content.index('{')
                    json_end = content.rindex('}') + 1
                    json_str = content[json_start:json_end]
                    sentiment_data = json.loads(json_str)
                else:
                    sentiment_data = json.loads(content)
                
                return {
                    'sentiment': sentiment_data.get('sentiment', 'neutral'),
                    'sentiment_score': float(sentiment_data.get('sentiment_score', 0.0)),
                    'risk_level': sentiment_data.get('risk_level', 'medium'),
                    'affected_sectors': sentiment_data.get('affected_sectors', []),
                    'affected_stocks': sentiment_data.get('affected_stocks', []),
                    'portfolio_impact': sentiment_data.get('portfolio_impact', 'Impact unclear'),
                    'key_topics': sentiment_data.get('key_topics', []),
                    'confidence': float(sentiment_data.get('confidence', 0.5)),
                    'analysis_timestamp': datetime.now().isoformat(),
                    'error': None
                }
            except json.JSONDecodeError as e:
                print(f"⚠️  JSON parse error: {str(e)}")
                print(f"   Response: {content[:200]}")
                return {
                    'sentiment': 'neutral',
                    'sentiment_score': 0.0,
                    'risk_level': 'medium',
                    'affected_sectors': [],
                    'affected_stocks': [],
                    'portfolio_impact': 'Analysis failed - JSON parse error',
                    'key_topics': [],
                    'confidence': 0.0,
                    'analysis_timestamp': datetime.now().isoformat(),
                    'error': f'JSON parse error: {str(e)}'
                }
        else:
            print(f"❌ API Error: {response.status_code} - {response.text[:200]}")
            return {
                'sentiment': 'neutral',
                'sentiment_score': 0.0,
                'risk_level': 'medium',
                'affected_sectors': [],
                'affected_stocks': [],
                'portfolio_impact': f'API error: {response.status_code}',
                'key_topics': [],
                'confidence': 0.0,
                'analysis_timestamp': datetime.now().isoformat(),
                'error': f'API error: {response.status_code}'
            }
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return {
            'sentiment': 'neutral',
            'sentiment_score': 0.0,
            'risk_level': 'medium',
            'affected_sectors': [],
            'affected_stocks': [],
            'portfolio_impact': f'Exception: {str(e)[:100]}',
            'key_topics': [],
            'confidence': 0.0,
            'analysis_timestamp': datetime.now().isoformat(),
            'error': str(e)
        }

# COMMAND ----------

# Test the sentiment analysis function
print("🧪 Testing sentiment analysis...")
test_result = analyze_sentiment_with_llm(
    "Federal Reserve signals potential rate cuts amid cooling inflation",
    "The Federal Reserve indicated it may lower interest rates in 2024 as inflation shows signs of moderating. Markets rallied on the news.",
    "https://example.com"
)
print(json.dumps(test_result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Get Unprocessed News Articles

# COMMAND ----------

# Get recent articles that haven't been analyzed yet
news_df = spark.sql("""
    SELECT 
        n.article_id,
        n.title,
        n.description,
        n.source,
        n.published_at,
        n.url,
        n.all_symbols,
        n.sentiment_score_av,
        n.sentiment_label_av,
        n.ingestion_timestamp
    FROM riskbricks.bronze.news_raw n
    LEFT JOIN riskbricks.silver.news_sentiment s
        ON n.article_id = s.article_id
    WHERE s.article_id IS NULL
        AND n.published_at >= CURRENT_DATE() - INTERVAL 7 DAYS
    ORDER BY n.published_at DESC
    LIMIT 100
""")

article_count = news_df.count()
print(f"📊 Found {article_count} unprocessed articles")

if article_count == 0:
    print("✅ All articles are already processed!")
    dbutils.notebook.exit("No new articles to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Process Articles with LLM (Batch Processing)

# COMMAND ----------

# Register UDF for sentiment analysis
sentiment_schema = StructType([
    StructField("sentiment", StringType()),
    StructField("sentiment_score", DoubleType()),
    StructField("risk_level", StringType()),
    StructField("affected_sectors", ArrayType(StringType())),
    StructField("affected_stocks", ArrayType(StringType())),
    StructField("portfolio_impact", StringType()),
    StructField("key_topics", ArrayType(StringType())),
    StructField("confidence", DoubleType()),
    StructField("analysis_timestamp", StringType()),
    StructField("error", StringType())
])

@F.udf(returnType=sentiment_schema)
def analyze_sentiment_udf(title, description, url):
    return analyze_sentiment_with_llm(title, description, url)

# COMMAND ----------

# Process articles (with rate limiting)
import time

print(f"🤖 Analyzing {article_count} articles with Llama 3.3 70B...")
print("⏱️  This may take a few minutes...")

# Process in small batches to avoid overwhelming the API
batch_size = 10
results = []

for i in range(0, article_count, batch_size):
    batch = news_df.limit(batch_size).offset(i)
    
    print(f"   Processing batch {i//batch_size + 1}/{(article_count-1)//batch_size + 1}...")
    
    # Apply sentiment analysis
    batch_with_sentiment = batch.withColumn(
        "sentiment_analysis",
        analyze_sentiment_udf(F.col("title"), F.col("description"), F.col("url"))
    )
    
    results.append(batch_with_sentiment)
    
    # Rate limiting - wait 1 second between batches
    if i + batch_size < article_count:
        time.sleep(1)

# Combine all batches
sentiment_df = results[0]
for batch in results[1:]:
    sentiment_df = sentiment_df.union(batch)

print(f"✅ Sentiment analysis complete for {article_count} articles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Expand Nested Sentiment Data

# COMMAND ----------

# Expand the sentiment analysis struct
sentiment_expanded = sentiment_df.select(
    F.col("article_id"),
    F.col("title"),
    F.col("description"),
    F.col("source"),
    F.col("published_at"),
    F.col("url"),
    F.col("all_symbols"),
    F.col("sentiment_score_av"),
    F.col("sentiment_label_av"),
    F.col("sentiment_analysis.sentiment").alias("sentiment"),
    F.col("sentiment_analysis.sentiment_score").alias("sentiment_score"),
    F.col("sentiment_analysis.risk_level").alias("risk_level"),
    F.col("sentiment_analysis.affected_sectors").alias("affected_sectors"),
    F.col("sentiment_analysis.affected_stocks").alias("affected_stocks"),
    F.col("sentiment_analysis.portfolio_impact").alias("portfolio_impact"),
    F.col("sentiment_analysis.key_topics").alias("key_topics"),
    F.col("sentiment_analysis.confidence").alias("confidence"),
    F.col("sentiment_analysis.error").alias("analysis_error"),
    F.lit(datetime.now()).alias("analyzed_at")
)

# COMMAND ----------

# Show sample results
print("📊 Sample sentiment analysis results:")
sentiment_expanded.select(
    "title", "sentiment", "sentiment_score", "risk_level", "affected_sectors", "portfolio_impact"
).show(5, truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Silver Layer

# COMMAND ----------

table_name = f"{catalog}.silver.news_sentiment"

# Create table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        article_id STRING,
        title STRING,
        description STRING,
        source STRING,
        published_at TIMESTAMP,
        url STRING,
        all_symbols ARRAY<STRING>,
        sentiment_score_av DOUBLE,
        sentiment_label_av STRING,
        sentiment STRING,
        sentiment_score DOUBLE,
        risk_level STRING,
        affected_sectors ARRAY<STRING>,
        affected_stocks ARRAY<STRING>,
        portfolio_impact STRING,
        key_topics ARRAY<STRING>,
        confidence DOUBLE,
        analysis_error STRING,
        analyzed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'News articles with LLM-generated sentiment analysis'
""")

# Write data
sentiment_expanded.write.mode("append").saveAsTable(table_name)

total_records = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
print(f"""
✅ Sentiment data saved to {table_name}
   New analyses: {article_count}
   Total in table: {total_records}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Sentiment Analysis Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment distribution
# MAGIC SELECT 
# MAGIC   sentiment,
# MAGIC   COUNT(*) as count,
# MAGIC   AVG(sentiment_score) as avg_score,
# MAGIC   AVG(confidence) as avg_confidence
# MAGIC FROM riskbricks.silver.news_sentiment
# MAGIC WHERE analyzed_at >= CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC GROUP BY sentiment
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Risk level distribution
# MAGIC SELECT 
# MAGIC   risk_level,
# MAGIC   COUNT(*) as count
# MAGIC FROM riskbricks.silver.news_sentiment
# MAGIC WHERE analyzed_at >= CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC GROUP BY risk_level
# MAGIC ORDER BY 
# MAGIC   CASE risk_level
# MAGIC     WHEN 'high' THEN 1
# MAGIC     WHEN 'medium' THEN 2
# MAGIC     WHEN 'low' THEN 3
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most affected sectors
# MAGIC SELECT 
# MAGIC   sector,
# MAGIC   COUNT(*) as mention_count,
# MAGIC   AVG(sentiment_score) as avg_sentiment
# MAGIC FROM riskbricks.silver.news_sentiment
# MAGIC LATERAL VIEW explode(affected_sectors) t as sector
# MAGIC WHERE analyzed_at >= CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC GROUP BY sector
# MAGIC ORDER BY mention_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent high-risk articles
# MAGIC SELECT 
# MAGIC   title,
# MAGIC   sentiment,
# MAGIC   sentiment_score,
# MAGIC   risk_level,
# MAGIC   affected_sectors,
# MAGIC   portfolio_impact,
# MAGIC   published_at
# MAGIC FROM riskbricks.silver.news_sentiment
# MAGIC WHERE risk_level = 'high'
# MAGIC   AND analyzed_at >= CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC ORDER BY sentiment_score ASC
# MAGIC LIMIT 10;

# COMMAND ----------

print("""
================================================================================
✅ SENTIMENT ANALYSIS COMPLETE!
================================================================================

🤖 LLM: Llama 3.3 70B Instruct (Databricks Foundation Models)
📊 Articles analyzed: {count}
💾 Table: riskbricks.silver.news_sentiment

📋 Next Steps:
   1. Review sentiment distribution above
   2. Run 06_geopolitical_stress.py to identify risk events
   3. Create UC functions for news queries
   4. Integrate with AI agent and dashboard
   
💡 Sentiment Scores:
   +1.0  = Very Bullish 🚀
    0.0  = Neutral ➖
   -1.0  = Very Bearish 📉

================================================================================
""".format(count=article_count))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'articles_analyzed': article_count,
    'total_with_sentiment': total_records
}))
