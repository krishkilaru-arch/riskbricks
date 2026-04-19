# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 News Ingestion - Real-Time Market Intelligence
# MAGIC
# MAGIC **Purpose**: Fetch and store market news for portfolio risk analysis
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - NewsAPI (100 free requests/day)
# MAGIC - RSS Feeds (Unlimited, free)
# MAGIC - Alpha Vantage News (500 free requests/day)
# MAGIC
# MAGIC **Output**: `riskbricks.bronze.news_raw`
# MAGIC
# MAGIC **Run Frequency**: Hourly (or before demo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install feedparser requests beautifulsoup4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

import requests
import feedparser
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

# Database setup
catalog = "riskbricks"
schema = "bronze"

# Create schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 API Keys Setup
# MAGIC
# MAGIC Get your free API keys:
# MAGIC - **NewsAPI**: https://newsapi.org/register (100 req/day free)
# MAGIC - **Alpha Vantage**: https://www.alphavantage.co/support/#api-key (500 req/day free)

# COMMAND ----------

# Try to get from Databricks Secrets first, fallback to None
try:
    NEWSAPI_KEY = dbutils.secrets.get(scope="riskbricks", key="newsapi-key")
    print("✅ Using NewsAPI key from secrets")
except:
    NEWSAPI_KEY = None
    print("⚠️  NewsAPI key not found in secrets. Will use RSS feeds only.")

try:
    ALPHAVANTAGE_KEY = dbutils.secrets.get(scope="riskbricks", key="alphavantage-key")
    print("✅ Using Alpha Vantage key from secrets")
except:
    ALPHAVANTAGE_KEY = None
    print("⚠️  Alpha Vantage key not found in secrets. Will skip.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Get Portfolio Holdings (for targeted news)

# COMMAND ----------

# Get all symbols from portfolio holdings
holdings_df = spark.sql("""
    SELECT DISTINCT symbol 
    FROM riskbricks.gold.portfolio_holdings
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in holdings_df.collect()]
print(f"📊 Tracking news for {len(portfolio_symbols)} portfolio holdings")
print(f"📊 Symbols: {', '.join(portfolio_symbols[:10])}..." if len(portfolio_symbols) > 10 else ', '.join(portfolio_symbols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Function 1: Fetch from RSS Feeds (Always Free!)

# COMMAND ----------

def fetch_rss_news(feed_urls, max_articles=50):
    """Fetch news from RSS feeds"""
    articles = []
    
    for feed_name, feed_url in feed_urls.items():
        try:
            print(f"📡 Fetching from {feed_name}...")
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:max_articles]:
                article = {
                    'article_id': entry.get('id', entry.link),
                    'title': entry.get('title', ''),
                    'description': entry.get('summary', entry.get('description', '')),
                    'source': feed_name,
                    'author': entry.get('author', None),
                    'published_at': datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.now(),
                    'url': entry.get('link', ''),
                    'categories': [tag.term for tag in entry.get('tags', [])],
                    'ingestion_timestamp': datetime.now()
                }
                articles.append(article)
            
            print(f"   ✅ Fetched {len(feed.entries[:max_articles])} articles")
        except Exception as e:
            print(f"   ❌ Error fetching {feed_name}: {str(e)}")
    
    return articles

# COMMAND ----------

# Define RSS feeds (always free!)
rss_feeds = {
    'Reuters Business': 'http://feeds.reuters.com/reuters/businessNews',
    'Reuters Markets': 'http://feeds.reuters.com/reuters/marketsNews',
    'Yahoo Finance': 'https://finance.yahoo.com/news/rssindex',
    'CNBC Top News': 'https://www.cnbc.com/id/100003114/device/rss/rss.html',
    'MarketWatch': 'http://feeds.marketwatch.com/marketwatch/topstories/',
    'Investing.com': 'https://www.investing.com/rss/news.rss'
}

# Fetch RSS news
print("📡 Fetching RSS feeds...")
rss_articles = fetch_rss_news(rss_feeds, max_articles=20)
print(f"✅ Total RSS articles fetched: {len(rss_articles)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Function 2: Fetch from NewsAPI (100 free/day)

# COMMAND ----------

def fetch_newsapi(api_key, keywords, days_back=1, page_size=20):
    """Fetch news from NewsAPI"""
    if not api_key:
        print("⚠️  No NewsAPI key provided. Skipping.")
        return []
    
    articles = []
    base_url = "https://newsapi.org/v2/everything"
    
    from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    try:
        print(f"📡 Fetching from NewsAPI...")
        response = requests.get(base_url, params={
            'q': ' OR '.join(keywords),
            'from': from_date,
            'sortBy': 'publishedAt',
            'language': 'en',
            'pageSize': page_size,
            'apiKey': api_key
        })
        
        if response.status_code == 200:
            data = response.json()
            
            for article in data.get('articles', []):
                articles.append({
                    'article_id': article.get('url', ''),
                    'title': article.get('title', ''),
                    'description': article.get('description', ''),
                    'source': article.get('source', {}).get('name', 'NewsAPI'),
                    'author': article.get('author'),
                    'published_at': datetime.strptime(article['publishedAt'][:19], '%Y-%m-%dT%H:%M:%S') if article.get('publishedAt') else datetime.now(),
                    'url': article.get('url', ''),
                    'categories': [],
                    'ingestion_timestamp': datetime.now()
                })
            
            print(f"   ✅ Fetched {len(articles)} articles")
        else:
            print(f"   ❌ NewsAPI error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"   ❌ Error fetching NewsAPI: {str(e)}")
    
    return articles

# COMMAND ----------

# Fetch NewsAPI (if key available)
newsapi_keywords = [
    'stock market', 'Federal Reserve', 'inflation', 'recession',
    'geopolitical risk', 'Greenland', 'trade war', 'tech sector',
    'S&P 500', 'earnings', 'IPO', 'merger'
] + portfolio_symbols[:5]  # Add top 5 portfolio stocks

newsapi_articles = fetch_newsapi(NEWSAPI_KEY, newsapi_keywords, days_back=1, page_size=20)
print(f"✅ Total NewsAPI articles fetched: {len(newsapi_articles)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Function 3: Fetch from Alpha Vantage News (500 free/day)

# COMMAND ----------

def fetch_alphavantage_news(api_key, symbols, max_per_symbol=5):
    """Fetch stock-specific news from Alpha Vantage"""
    if not api_key:
        print("⚠️  No Alpha Vantage key provided. Skipping.")
        return []
    
    articles = []
    base_url = "https://www.alphavantage.co/query"
    
    for symbol in symbols[:10]:  # Limit to 10 symbols to avoid rate limits
        try:
            print(f"📡 Fetching news for {symbol}...")
            response = requests.get(base_url, params={
                'function': 'NEWS_SENTIMENT',
                'tickers': symbol,
                'apikey': api_key,
                'limit': max_per_symbol
            })
            
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get('feed', [])[:max_per_symbol]:
                    articles.append({
                        'article_id': item.get('url', ''),
                        'title': item.get('title', ''),
                        'description': item.get('summary', ''),
                        'source': item.get('source', 'Alpha Vantage'),
                        'author': None,
                        'published_at': datetime.strptime(item['time_published'], '%Y%m%dT%H%M%S') if item.get('time_published') else datetime.now(),
                        'url': item.get('url', ''),
                        'categories': [symbol],
                        'symbols': [t['ticker'] for t in item.get('ticker_sentiment', [])],
                        'sentiment_score_av': item.get('overall_sentiment_score'),  # Alpha Vantage's sentiment
                        'sentiment_label_av': item.get('overall_sentiment_label'),
                        'ingestion_timestamp': datetime.now()
                    })
                
                print(f"   ✅ Fetched {min(max_per_symbol, len(data.get('feed', [])))} articles")
            else:
                print(f"   ❌ Alpha Vantage error: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Error fetching {symbol}: {str(e)}")
    
    return articles

# COMMAND ----------

# Fetch Alpha Vantage news (if key available)
alphavantage_articles = fetch_alphavantage_news(ALPHAVANTAGE_KEY, portfolio_symbols[:10], max_per_symbol=3)
print(f"✅ Total Alpha Vantage articles fetched: {len(alphavantage_articles)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Combine All News Sources

# COMMAND ----------

# Combine all articles
all_articles = rss_articles + newsapi_articles + alphavantage_articles

print(f"""
📊 News Ingestion Summary:
   RSS Feeds: {len(rss_articles)}
   NewsAPI: {len(newsapi_articles)}
   Alpha Vantage: {len(alphavantage_articles)}
   ─────────────────────────
   Total: {len(all_articles)} articles
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Bronze Layer

# COMMAND ----------

if len(all_articles) == 0:
    print("⚠️  No articles fetched. Skipping save.")
    dbutils.notebook.exit("No articles to save")

# Define schema
news_schema = StructType([
    StructField("article_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source", StringType(), True),
    StructField("author", StringType(), True),
    StructField("published_at", TimestampType(), True),
    StructField("url", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("symbols", ArrayType(StringType()), True),
    StructField("sentiment_score_av", DoubleType(), True),  # From Alpha Vantage
    StructField("sentiment_label_av", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

# Create DataFrame
news_df = spark.createDataFrame(all_articles, schema=news_schema)

# Add extracted symbols from title/description
news_df = news_df.withColumn(
    "extracted_symbols",
    F.array_distinct(
        F.filter(
            F.array(*[F.lit(s) for s in portfolio_symbols]),
            lambda x: F.col("title").contains(x) | F.col("description").contains(x)
        )
    )
)

# Merge symbols and extracted_symbols
news_df = news_df.withColumn(
    "all_symbols",
    F.array_distinct(F.concat(F.coalesce(F.col("symbols"), F.array()), F.col("extracted_symbols")))
)

# Drop intermediate column
news_df = news_df.drop("extracted_symbols")

# COMMAND ----------

# Show sample
print("📰 Sample articles:")
news_df.select("title", "source", "published_at", "all_symbols").show(5, truncate=60)

# COMMAND ----------

# Save to Delta table
table_name = f"{catalog}.{schema}.news_raw"

# Write with merge to avoid duplicates
news_df.createOrReplaceTempView("news_raw_temp")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        article_id STRING,
        title STRING,
        description STRING,
        source STRING,
        author STRING,
        published_at TIMESTAMP,
        url STRING,
        categories ARRAY<STRING>,
        symbols ARRAY<STRING>,
        all_symbols ARRAY<STRING>,
        sentiment_score_av DOUBLE,
        sentiment_label_av STRING,
        ingestion_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'Raw news articles from multiple sources'
""")

# Merge (upsert) to avoid duplicates
spark.sql(f"""
    MERGE INTO {table_name} as target
    USING news_raw_temp as source
    ON target.article_id = source.article_id
    WHEN NOT MATCHED THEN INSERT *
""")

# Count
total_records = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
new_records = news_df.count()

print(f"""
✅ News data saved to {table_name}
   New articles: {new_records}
   Total in table: {total_records}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Freshness Report

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   source,
# MAGIC   COUNT(*) as article_count,
# MAGIC   MAX(published_at) as latest_article,
# MAGIC   MIN(published_at) as oldest_article
# MAGIC FROM riskbricks.bronze.news_raw
# MAGIC GROUP BY source
# MAGIC ORDER BY article_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Preview Recent News

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   title,
# MAGIC   source,
# MAGIC   published_at,
# MAGIC   all_symbols,
# MAGIC   sentiment_label_av
# MAGIC FROM riskbricks.bronze.news_raw
# MAGIC WHERE published_at >= CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC ORDER BY published_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------

print("""
================================================================================
✅ NEWS INGESTION COMPLETE!
================================================================================

📊 Summary:
   - Articles ingested: {total}
   - Sources: RSS (free), NewsAPI (100/day), Alpha Vantage (500/day)
   - Table: riskbricks.bronze.news_raw
   
📋 Next Steps:
   1. Run 05_news_sentiment.py to analyze sentiment with LLM
   2. Run 06_geopolitical_stress.py to identify risk events
   3. Check dashboard for live news feed
   
💡 To get API keys (free):
   NewsAPI: https://newsapi.org/register
   Alpha Vantage: https://www.alphavantage.co/support/#api-key

================================================================================
""".format(total=new_records))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'articles_ingested': new_records,
    'total_articles': total_records
}))
