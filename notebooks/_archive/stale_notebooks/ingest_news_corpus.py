# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 Build News Corpus for RAG Agent
# MAGIC
# MAGIC **Purpose**: Create a searchable news corpus with actual headlines and summaries
# MAGIC
# MAGIC **Data Sources**:
# MAGIC 1. **NewsAPI** - Recent headlines (last 30 days for free tier)
# MAGIC 2. **SEC 8-K Filings** - Material events (free, official)
# MAGIC 3. **FinViz RSS** - Free financial news aggregator
# MAGIC
# MAGIC **Output**: `riskbricks.bronze.news_corpus` with actual article content

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install feedparser beautifulsoup4 requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import feedparser
from bs4 import BeautifulSoup
import hashlib
import time

# Database setup
catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

print(f"✅ Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Get Portfolio Symbols

# COMMAND ----------

# Get symbols from portfolio
symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_to_company = {row.symbol: row.company_name for row in symbols_df.collect()}

print(f"📊 Building news corpus for {len(portfolio_symbols)} stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Source 1: FinViz RSS Feeds (FREE, No API Key)
# MAGIC
# MAGIC FinViz aggregates news from multiple sources for each stock.

# COMMAND ----------

def fetch_finviz_news(symbol, max_articles=20):
    """
    Fetch recent news headlines from FinViz for a stock
    Returns list of news articles with headline, source, and date
    """
    articles = []
    
    try:
        # FinViz stock news page
        url = f"https://finviz.com/quote.ashx?t={symbol}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return articles
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find news table
        news_table = soup.find('table', class_='fullview-news-outer')
        
        if not news_table:
            return articles
        
        # Parse news rows
        rows = news_table.find_all('tr')
        
        for row in rows[:max_articles]:
            try:
                # Get date/time
                date_cell = row.find('td', class_='news-date-cell')
                date_text = date_cell.text.strip() if date_cell else ""
                
                # Get headline and link
                link = row.find('a', class_='tab-link-news')
                if link:
                    headline = link.text.strip()
                    url = link.get('href', '')
                    
                    # Get source
                    source_span = row.find('span', class_='news-source')
                    source = source_span.text.strip() if source_span else "FinViz"
                    
                    articles.append({
                        'symbol': symbol,
                        'company_name': symbol_to_company.get(symbol, symbol),
                        'headline': headline,
                        'summary': headline,  # FinViz doesn't provide summaries
                        'source': source,
                        'url': url,
                        'published_date': datetime.now().strftime('%Y-%m-%d'),  # Approximate
                        'ingestion_timestamp': datetime.now()
                    })
            except Exception as e:
                continue
    
    except Exception as e:
        print(f"  ⚠️ Error fetching FinViz news for {symbol}: {str(e)}")
    
    return articles

# Test with one symbol
print("Testing FinViz news fetch...")
test_articles = fetch_finviz_news("AAPL", max_articles=5)
print(f"  ✅ Found {len(test_articles)} articles for AAPL")
if test_articles:
    print(f"  📰 Sample: {test_articles[0]['headline'][:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Source 2: Yahoo Finance RSS (FREE)

# COMMAND ----------

def fetch_yahoo_rss_news(symbol, max_articles=10):
    """
    Fetch news from Yahoo Finance RSS feed
    """
    articles = []
    
    try:
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        
        feed = feedparser.parse(rss_url)
        
        for entry in feed.entries[:max_articles]:
            try:
                # Parse published date
                pub_date = entry.get('published', '')
                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_date_str = pub_datetime.strftime('%Y-%m-%d')
                    except:
                        pub_date_str = datetime.now().strftime('%Y-%m-%d')
                else:
                    pub_date_str = datetime.now().strftime('%Y-%m-%d')
                
                articles.append({
                    'symbol': symbol,
                    'company_name': symbol_to_company.get(symbol, symbol),
                    'headline': entry.get('title', ''),
                    'summary': entry.get('summary', entry.get('title', '')),
                    'source': 'Yahoo Finance',
                    'url': entry.get('link', ''),
                    'published_date': pub_date_str,
                    'ingestion_timestamp': datetime.now()
                })
            except Exception as e:
                continue
    
    except Exception as e:
        print(f"  ⚠️ Error fetching Yahoo RSS for {symbol}: {str(e)}")
    
    return articles

# Test
print("Testing Yahoo RSS news fetch...")
test_articles = fetch_yahoo_rss_news("COST", max_articles=5)
print(f"  ✅ Found {len(test_articles)} articles for COST")
if test_articles:
    print(f"  📰 Sample: {test_articles[0]['headline'][:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Source 3: Google News RSS (FREE)

# COMMAND ----------

def fetch_google_news(symbol, company_name, max_articles=10):
    """
    Fetch news from Google News RSS
    """
    articles = []
    
    try:
        # Use company name for better results
        query = f"{company_name} stock".replace(" ", "+")
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        
        feed = feedparser.parse(rss_url)
        
        for entry in feed.entries[:max_articles]:
            try:
                # Parse published date
                pub_date = entry.get('published', '')
                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_date_str = pub_datetime.strftime('%Y-%m-%d')
                    except:
                        pub_date_str = datetime.now().strftime('%Y-%m-%d')
                else:
                    pub_date_str = datetime.now().strftime('%Y-%m-%d')
                
                # Extract source from title (Google News format: "Title - Source")
                title = entry.get('title', '')
                source = 'Google News'
                if ' - ' in title:
                    parts = title.rsplit(' - ', 1)
                    if len(parts) == 2:
                        title = parts[0]
                        source = parts[1]
                
                articles.append({
                    'symbol': symbol,
                    'company_name': company_name,
                    'headline': title,
                    'summary': entry.get('summary', title),
                    'source': source,
                    'url': entry.get('link', ''),
                    'published_date': pub_date_str,
                    'ingestion_timestamp': datetime.now()
                })
            except Exception as e:
                continue
    
    except Exception as e:
        print(f"  ⚠️ Error fetching Google News for {symbol}: {str(e)}")
    
    return articles

# Test
print("Testing Google News fetch...")
test_articles = fetch_google_news("COST", "Costco Wholesale", max_articles=5)
print(f"  ✅ Found {len(test_articles)} articles for COST")
if test_articles:
    print(f"  📰 Sample: {test_articles[0]['headline'][:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Fetch News for All Portfolio Stocks

# COMMAND ----------

print("📰 Fetching news for all portfolio stocks...")
print(f"⏱️  This will take ~10-20 minutes for {len(portfolio_symbols)} stocks")
print("")

all_articles = []
processed = 0
errors = 0

# Process top 100 stocks (to avoid rate limits)
symbols_to_process = portfolio_symbols[:100]

for symbol in symbols_to_process:
    company_name = symbol_to_company.get(symbol, symbol)
    symbol_articles = []
    
    # Fetch from all sources
    try:
        # Yahoo RSS (most reliable)
        yahoo_articles = fetch_yahoo_rss_news(symbol, max_articles=5)
        symbol_articles.extend(yahoo_articles)
        
        # Google News
        google_articles = fetch_google_news(symbol, company_name, max_articles=5)
        symbol_articles.extend(google_articles)
        
        # Add small delay to avoid rate limits
        time.sleep(0.5)
        
        if symbol_articles:
            all_articles.extend(symbol_articles)
            processed += 1
            if processed % 10 == 0:
                print(f"  📊 Processed {processed}/{len(symbols_to_process)} stocks, {len(all_articles)} articles total")
        else:
            errors += 1
    
    except Exception as e:
        errors += 1
        print(f"  ⚠️ Error processing {symbol}: {str(e)}")

print(f"\n✅ Completed: {processed} stocks, {len(all_articles)} articles, {errors} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Bronze Layer

# COMMAND ----------

if len(all_articles) == 0:
    print("⚠️  No articles collected. Check network and API access.")
    dbutils.notebook.exit("No articles to save")

# Convert to DataFrame
articles_df = pd.DataFrame(all_articles)

# Create unique article ID
articles_df['article_id'] = articles_df.apply(
    lambda row: hashlib.md5(
        f"{row['symbol']}{row['headline']}{row['published_date']}".encode()
    ).hexdigest()[:16],
    axis=1
)

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(articles_df)

# Remove duplicates
spark_df = spark_df.dropDuplicates(['article_id'])

# Show sample
print("📰 Sample articles:")
spark_df.select("published_date", "symbol", "headline", "source").show(10, truncate=60)

# COMMAND ----------

# Save to table
table_name = f"{catalog}.bronze.news_corpus"

# Write to table
spark_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

# Add comment
spark.sql(f"COMMENT ON TABLE {table_name} IS 'News corpus with headlines and summaries for RAG agent'")

total_records = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
print(f"""
✅ Saved to {table_name}
   Total articles: {total_records}
   Unique symbols: {spark_df.select('symbol').distinct().count()}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Corpus Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Articles by source
# MAGIC SELECT 
# MAGIC   source,
# MAGIC   COUNT(*) as article_count,
# MAGIC   COUNT(DISTINCT symbol) as unique_stocks
# MAGIC FROM riskbricks.bronze.news_corpus
# MAGIC GROUP BY source
# MAGIC ORDER BY article_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top stocks by coverage
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   COUNT(*) as article_count,
# MAGIC   MIN(published_date) as earliest,
# MAGIC   MAX(published_date) as latest
# MAGIC FROM riskbricks.bronze.news_corpus
# MAGIC GROUP BY symbol, company_name
# MAGIC ORDER BY article_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample headlines for Costco
# MAGIC SELECT 
# MAGIC   published_date,
# MAGIC   headline,
# MAGIC   source
# MAGIC FROM riskbricks.bronze.news_corpus
# MAGIC WHERE symbol = 'COST'
# MAGIC ORDER BY published_date DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ News Corpus Complete!
# MAGIC
# MAGIC **Next Steps**:
# MAGIC 1. Run `notebooks/03_gold/rag/create_vector_index.py` to create Vector Search index
# MAGIC 2. Run `notebooks/03_gold/rag/create_rag_agent.py` to build RAG agent
# MAGIC 3. The agent can then answer: "What happened to Costco on September 26, 2024?"

# COMMAND ----------

print(f"""
================================================================================
✅ NEWS CORPUS BUILD COMPLETE!
================================================================================

📊 Summary:
   - Total articles: {total_records}
   - Unique stocks covered: {spark_df.select('symbol').distinct().count()}
   - Sources: Yahoo Finance RSS, Google News
   - Table: riskbricks.bronze.news_corpus
   
🎯 What This Enables:
   - Actual headlines and summaries (not just sentiment)
   - Source attribution (Reuters, Bloomberg, etc.)
   - URL links for verification
   
🔄 Next Steps:
   1. Create Vector Search index for semantic search
   2. Build RAG agent that combines:
      - GDELT (sentiment + timing signals)
      - News Corpus (actual headlines)
      - LLM (synthesis and Q&A)

================================================================================
""")

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'articles_collected': total_records,
    'stocks_covered': spark_df.select('symbol').distinct().count(),
    'table': 'riskbricks.bronze.news_corpus'
}))
