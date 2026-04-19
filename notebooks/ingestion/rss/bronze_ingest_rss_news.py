# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 Daily RSS News Loader (Yahoo + Google)
# MAGIC
# MAGIC **Purpose**: Incremental daily ingestion of real-time news for RAG
# MAGIC
# MAGIC **Sources**:
# MAGIC - Yahoo Finance RSS
# MAGIC - Google News RSS
# MAGIC
# MAGIC **Output**:
# MAGIC - `riskbricks.bronze.news_rss_all`

# COMMAND ----------

# Widgets (run this cell first)
dbutils.widgets.text("max_yahoo_articles", "50", "Max Yahoo articles per symbol")
dbutils.widgets.text("max_google_articles", "30", "Max Google articles per symbol")
dbutils.widgets.text("sleep_seconds", "0.2", "Sleep between symbols (seconds)")
dbutils.widgets.text("finnhub_days_back", "7", "Finnhub news lookback days")
dbutils.widgets.text("finnhub_sleep_seconds", "0.1", "Sleep between Finnhub calls (seconds)")
dbutils.widgets.text("finnhub_market_categories", "general,forex,crypto,merger,economic,top,politics", "Finnhub market categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install feedparser beautifulsoup4 requests lxml wikipedia-api

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup
import hashlib
import time
import calendar

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

max_yahoo_articles = int(dbutils.widgets.get("max_yahoo_articles"))
max_google_articles = int(dbutils.widgets.get("max_google_articles"))
sleep_seconds = float(dbutils.widgets.get("sleep_seconds"))
finnhub_days_back = int(dbutils.widgets.get("finnhub_days_back"))
finnhub_sleep_seconds = float(dbutils.widgets.get("finnhub_sleep_seconds"))
finnhub_market_categories = [
    c.strip() for c in dbutils.widgets.get("finnhub_market_categories").split(",") if c.strip()
]

try:
    finnhub_token = dbutils.secrets.get(scope="riskbricks", key="finnhub-token").strip()
except Exception:
    finnhub_token = ""

print(f"✅ Using catalog: {catalog}")
print(f"✅ Yahoo max: {max_yahoo_articles}, Google max: {max_google_articles}")
print(f"✅ Finnhub enabled: {bool(finnhub_token)} (days_back={finnhub_days_back})")
print(f"✅ Finnhub market categories: {finnhub_market_categories}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Get Portfolio Symbols

# COMMAND ----------

symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM {catalog}.gold.company_universe
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_to_company = {row.symbol: row.company_name for row in symbols_df.collect()}
symbol_to_sector = {row.symbol: row.sector for row in symbols_df.collect()}

print(f"📊 Daily RSS ingestion for {len(portfolio_symbols)} stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Yahoo Finance RSS

# COMMAND ----------

def fetch_yahoo_news(symbol, max_articles=10):
    articles = []
    try:
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(rss_url)

        for entry in feed.entries[:max_articles]:
            try:
                pub_date = entry.get("published", "")
                pub_date_str = datetime.now().strftime("%Y-%m-%d")

                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_date_str = pub_datetime.strftime("%Y-%m-%d")
                    except Exception:
                        pass

                summary = entry.get("summary", entry.get("title", ""))
                if summary and not summary.strip().lower().startswith("http"):
                    summary = BeautifulSoup(summary, "html.parser").get_text()[:1000]

                articles.append({
                    "symbol": symbol,
                    "company_name": symbol_to_company.get(symbol, symbol),
                    "sector": symbol_to_sector.get(symbol, "Unknown"),
                    "doc_type": "news",
                    "title": entry.get("title", ""),
                    "content": summary,
                    "source": "Yahoo Finance",
                    "url": entry.get("link", ""),
                    "published_date": pub_date_str,
                    "ingestion_timestamp": datetime.now()
                })
            except Exception:
                continue
    except Exception:
        pass
    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Google News RSS

# COMMAND ----------

def fetch_google_news(symbol, company_name, max_articles=10):
    articles = []
    try:
        query = f"{company_name} stock".replace(" ", "+").replace("&", "and")
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(rss_url)

        for entry in feed.entries[:max_articles]:
            try:
                pub_date = entry.get("published", "")
                pub_date_str = datetime.now().strftime("%Y-%m-%d")

                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_date_str = pub_datetime.strftime("%Y-%m-%d")
                    except Exception:
                        pass

                title = entry.get("title", "")
                source = "Google News"
                if " - " in title:
                    parts = title.rsplit(" - ", 1)
                    if len(parts) == 2:
                        title = parts[0]
                        source = parts[1]

                summary = entry.get("summary", title)
                if summary and not summary.strip().lower().startswith("http"):
                    summary = BeautifulSoup(summary, "html.parser").get_text()[:1000]

                articles.append({
                    "symbol": symbol,
                    "company_name": company_name,
                    "sector": symbol_to_sector.get(symbol, "Unknown"),
                    "doc_type": "news",
                    "title": title,
                    "content": summary,
                    "source": source,
                    "url": entry.get("link", ""),
                    "published_date": pub_date_str,
                    "ingestion_timestamp": datetime.now()
                })
            except Exception:
                continue
    except Exception:
        pass
    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌐 Finnhub News (Company + Market)

# COMMAND ----------

def fetch_finnhub_company_news(token, symbol, company_name, sector, days_back=7, sleep_seconds=0.1):
    """Fetch company news from Finnhub for the last N days, per-day calls to maximize coverage."""
    if not token:
        return []

    articles = []
    base_url = "https://finnhub.io/api/v1/company-news"
    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=days_back)

    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        try:
            resp = requests.get(
                base_url,
                params={"symbol": symbol, "from": date_str, "to": date_str, "token": token},
                timeout=30,
            )
            if resp.status_code != 200:
                current_dt += timedelta(days=1)
                continue
            data = resp.json() or []
        except Exception:
            current_dt += timedelta(days=1)
            continue

        for item in data:
            try:
                ts = item.get("datetime")
                if not ts:
                    continue
                pub_dt = datetime.fromtimestamp(int(ts), timezone.utc)
                headline = item.get("headline", "")
                summary = item.get("summary", "")[:1000]
                source = item.get("source", "Finnhub")
                url = item.get("url", "")

                articles.append({
                    "symbol": symbol,
                    "company_name": company_name,
                    "sector": sector,
                    "doc_type": "company_news",
                    "title": headline,
                    "content": summary,
                    "source": source,
                    "url": url,
                    "published_date": pub_dt.strftime("%Y-%m-%d"),
                    "ingestion_timestamp": datetime.now()
                })
            except Exception:
                continue

        current_dt += timedelta(days=1)
        time.sleep(sleep_seconds)

    return articles

def fetch_finnhub_market_news(token, days_back=7, max_items=200, categories=None):
    """Fetch market news from Finnhub categories (last N days)."""
    if not token:
        return []

    articles = []
    base_url = "https://finnhub.io/api/v1/news"
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)
    categories = categories or ["general"]

    for category in categories:
        try:
            resp = requests.get(
                base_url,
                params={"category": category, "token": token},
                timeout=30,
            )
            if resp.status_code != 200:
                continue
            data = resp.json() or []
        except Exception:
            continue

        for item in data:
            try:
                ts = item.get("datetime")
                if not ts:
                    continue
                pub_dt = datetime.fromtimestamp(int(ts), timezone.utc)
                if pub_dt < start_dt or pub_dt > end_dt:
                    continue
                headline = item.get("headline", "")
                summary = item.get("summary", "")[:1000]
                source = item.get("source", "Finnhub")
                url = item.get("url", "")

                articles.append({
                    "symbol": "MARKET",
                    "company_name": "Market",
                    "sector": "Macro",
                    "doc_type": f"market_news_{category}",
                    "title": headline,
                    "content": summary,
                    "source": source,
                    "url": url,
                    "published_date": pub_dt.strftime("%Y-%m-%d"),
                    "ingestion_timestamp": datetime.now()
                })
            except Exception:
                continue

    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Fetch Daily RSS News

# COMMAND ----------

print("=" * 70)
print("📰 DAILY RSS INGESTION")
print("=" * 70)

yahoo_articles = []
google_articles = []
finnhub_articles = []
generic_articles = []
processed = 0
errors = 0

# Add general market news (once per run)
market_news = fetch_finnhub_market_news(
    finnhub_token,
    finnhub_days_back,
    categories=finnhub_market_categories
)
if market_news:
    finnhub_articles.extend(market_news)
    print(f"🌐 Added {len(market_news)} Finnhub market news items")

# Generic RSS feeds (market/news/tech/etc.)
GENERIC_RSS_FEEDS = [
    {"name": "BBC World", "url": "https://feeds.bbci.co.uk/news/world/rss.xml", "doc_type": "rss_general"},
    {"name": "BBC Business", "url": "https://feeds.bbci.co.uk/news/business/rss.xml", "doc_type": "rss_business"},
    {"name": "Yahoo Finance Top", "url": "https://finance.yahoo.com/rss/topstories", "doc_type": "rss_markets"},
    {"name": "CNBC Top", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "doc_type": "rss_markets"},
    {"name": "MarketWatch Top", "url": "https://feeds.marketwatch.com/marketwatch/topstories/", "doc_type": "rss_markets"},
    {"name": "Seeking Alpha", "url": "https://seekingalpha.com/feed.xml", "doc_type": "rss_markets"},
    {"name": "TechCrunch", "url": "https://techcrunch.com/feed/", "doc_type": "rss_tech"},
    {"name": "The Verge", "url": "https://www.theverge.com/rss/index.xml", "doc_type": "rss_tech"},
    {"name": "Ars Technica", "url": "https://feeds.arstechnica.com/arstechnica/index", "doc_type": "rss_tech"},
    {"name": "Hacker News", "url": "https://news.ycombinator.com/rss", "doc_type": "rss_tech"},
    {"name": "Financial Times", "url": "https://www.ft.com/rss/home", "doc_type": "rss_business"},
    {"name": "NPR News", "url": "https://feeds.npr.org/1001/rss.xml", "doc_type": "rss_general"},
    {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "doc_type": "rss_general"},
    {"name": "Guardian World", "url": "https://www.theguardian.com/world/rss", "doc_type": "rss_general"},
    {"name": "Benzinga", "url": "https://www.benzinga.com/feed", "doc_type": "rss_markets"},
    {"name": "Forbes Business", "url": "https://www.forbes.com/business/feed2/", "doc_type": "rss_business"},
    {"name": "Wired", "url": "https://www.wired.com/feed/rss", "doc_type": "rss_tech"},
    {"name": "CNET News", "url": "https://www.cnet.com/rss/news/", "doc_type": "rss_tech"},
    {"name": "VentureBeat", "url": "https://venturebeat.com/feed/", "doc_type": "rss_tech"},
    {"name": "The Hacker News", "url": "https://feeds.feedburner.com/TheHackersNews", "doc_type": "rss_tech"},
    {"name": "Washington Post World", "url": "https://feeds.washingtonpost.com/rss/world", "doc_type": "rss_general"},
    {"name": "ABC News US", "url": "https://abcnews.go.com/abcnews/usheadlines", "doc_type": "rss_general"},
]

def _entry_date_to_str(entry):
    dt = None
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        if entry.get(key):
            dt = datetime(*entry.get(key)[:6], tzinfo=timezone.utc)
            break
    if not dt:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")

def fetch_generic_rss(feeds):
    records = []
    per_source_counts = {}
    for feed in feeds:
        count = 0
        try:
            parsed = feedparser.parse(feed["url"])
            for entry in parsed.entries:
                try:
                    title = entry.get("title", "")
                    summary = entry.get("summary", title)
                    if summary and not summary.strip().lower().startswith("http"):
                        summary = BeautifulSoup(summary, "html.parser").get_text()[:1000]
                    records.append({
                        "symbol": "MARKET",
                        "company_name": "Market",
                        "sector": "Macro",
                        "doc_type": feed["doc_type"],
                        "title": title,
                        "content": summary,
                        "source": feed["name"],
                        "feed_name": feed["name"],
                        "feed_url": feed["url"],
                        "url": entry.get("link", ""),
                        "published_date": _entry_date_to_str(entry),
                        "ingestion_timestamp": datetime.now()
                    })
                    count += 1
                except Exception:
                    continue
        except Exception:
            continue
        per_source_counts[feed["name"]] = count
    if per_source_counts:
        print("✅ RSS feed counts:")
        for name, count in sorted(per_source_counts.items(), key=lambda x: x[0]):
            print(f"  - {name}: {count}")
    return records

generic_articles = fetch_generic_rss(GENERIC_RSS_FEEDS)
if generic_articles:
    print(f"🧭 Added {len(generic_articles)} generic RSS items")

for i, symbol in enumerate(portfolio_symbols):
    company_name = symbol_to_company.get(symbol, symbol)
    sector = symbol_to_sector.get(symbol, "Unknown")
    symbol_articles = []
    try:
        finnhub = fetch_finnhub_company_news(
            finnhub_token,
            symbol,
            company_name,
            sector,
            days_back=finnhub_days_back,
            sleep_seconds=finnhub_sleep_seconds
        )
        yahoo = fetch_yahoo_news(symbol, max_articles=max_yahoo_articles)
        google = fetch_google_news(symbol, company_name, max_articles=max_google_articles)
        if finnhub:
            finnhub_articles.extend(finnhub)
        symbol_articles.extend(yahoo)
        symbol_articles.extend(google)

        if yahoo:
            yahoo_articles.extend(yahoo)
        if google:
            google_articles.extend(google)
            processed += 1

        if (i + 1) % 25 == 0:
            total_docs = len(yahoo_articles) + len(google_articles) + len(finnhub_articles)
            print(f"  📊 Processed {i+1}/{len(portfolio_symbols)} stocks, {total_docs} docs")

        time.sleep(sleep_seconds)
    except Exception:
        errors += 1

print("")
print(f"✅ Completed: {processed} stocks, {len(yahoo_articles)} Yahoo, {len(google_articles)} Google, {len(finnhub_articles)} Finnhub, {len(generic_articles)} Generic RSS, {errors} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Bronze (MERGE by doc_id)

# COMMAND ----------

def save_rss_table(records, table_name):
    if len(records) == 0:
        print(f"⚠️ No records for {table_name}")
        return

    df = pd.DataFrame(records)
    df["doc_id"] = df.apply(
        lambda row: hashlib.md5(
            f"{row['symbol']}{row['title']}{row['published_date']}{row['doc_type']}".encode()
        ).hexdigest()[:16],
        axis=1
    )

    spark_df = spark.createDataFrame(df) \
        .withColumn("published_date", F.to_date("published_date")) \
        .withColumn("source", F.coalesce(F.col("source"), F.lit("Unknown"))) \
        .dropDuplicates(["doc_id"])

    if not spark.catalog.tableExists(table_name):
        spark_df.write \
            .mode("overwrite") \
            .partitionBy("published_date", "source") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        spark_df.createOrReplaceTempView("rss_updates")
        spark.sql(f"""
            MERGE INTO {table_name} t
            USING rss_updates s
            ON t.doc_id = s.doc_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    total = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]["count"]
    print(f"✅ Saved RSS to {table_name}. Total rows: {total}")

all_articles = []
all_articles.extend(yahoo_articles)
all_articles.extend(google_articles)
all_articles.extend(finnhub_articles)
all_articles.extend(generic_articles)
save_rss_table(all_articles, f"{catalog}.bronze.news_rss_all")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

