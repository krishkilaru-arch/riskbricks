# Databricks notebook source
# MAGIC %md
# MAGIC # 📰 Build Comprehensive News & Document Corpus for RAG
# MAGIC 
# MAGIC **Purpose**: Create a rich, searchable corpus with multiple data sources
# MAGIC 
# MAGIC **Data Sources** (All FREE):
# MAGIC 
# MAGIC | Source | Data Type | Coverage |
# MAGIC |--------|-----------|----------|
# MAGIC | Yahoo Finance RSS | Stock news headlines | Real-time |
# MAGIC | Google News RSS | Broader financial news | Real-time |
# MAGIC | SEC EDGAR | 10-K, 10-Q, 8-K filings | Historical |
# MAGIC | Wikipedia | Company background | Static |
# MAGIC | Earnings Call Transcripts | Management commentary | Quarterly |
# MAGIC 
# MAGIC **Output**: `riskbricks.bronze.rag_corpus`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Install Dependencies

# COMMAND ----------

# MAGIC %pip install feedparser beautifulsoup4 requests lxml wikipedia-api
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
import xml.etree.ElementTree as ET
import json
import feedparser
from bs4 import BeautifulSoup
import hashlib
import time
import re
import wikipediaapi

# Database setup
catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

print(f"✅ Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Get Portfolio Symbols

# COMMAND ----------

# Get symbols from portfolio (limit to 3 stocks)
allowed_symbols = ["NVDA", "MSFT", "COST"]
allowed_list = ", ".join([f"'{s}'" for s in allowed_symbols])

symbols_df = spark.sql(f"""
    SELECT DISTINCT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
    WHERE symbol IN ({allowed_list})
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_to_company = {row.symbol: row.company_name for row in symbols_df.collect()}
symbol_to_sector = {row.symbol: row.sector for row in symbols_df.collect()}

print(f"📊 Building corpus for {len(portfolio_symbols)} stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Source 1: Yahoo Finance RSS (Stock News)

# COMMAND ----------

def fetch_yahoo_news(symbol, max_articles=10):
    """Fetch news from Yahoo Finance RSS feed"""
    articles = []
    
    try:
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(rss_url)
        
        for entry in feed.entries[:max_articles]:
            try:
                pub_date = entry.get('published', '')
                pub_date_str = datetime.now().strftime('%Y-%m-%d')
                
                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_date_str = pub_datetime.strftime('%Y-%m-%d')
                    except:
                        pass
                
                summary = entry.get('summary', entry.get('title', ''))
                if summary:
                    summary = BeautifulSoup(summary, 'html.parser').get_text()[:1000]
                
                articles.append({
                    'symbol': symbol,
                    'company_name': symbol_to_company.get(symbol, symbol),
                    'sector': symbol_to_sector.get(symbol, 'Unknown'),
                    'doc_type': 'news',
                    'title': entry.get('title', ''),
                    'content': summary,
                    'source': 'Yahoo Finance',
                    'url': entry.get('link', ''),
                    'published_date': pub_date_str,
                    'ingestion_timestamp': datetime.now()
                })
            except:
                continue
    except:
        pass
    
    return articles

# Test
print("Testing Yahoo News...")
test = fetch_yahoo_news("COST", 3)
print(f"  ✅ Found {len(test)} articles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📰 Source 2: Google News RSS

# COMMAND ----------

def fetch_google_news(symbol, company_name, max_articles=10):
    """Fetch news from Google News RSS"""
    articles = []
    
    try:
        query = f"{company_name} stock".replace(" ", "+").replace("&", "and")
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(rss_url)
        
        for entry in feed.entries[:max_articles]:
            try:
                pub_date = entry.get('published', '')
                pub_date_str = datetime.now().strftime('%Y-%m-%d')
                
                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_date_str = pub_datetime.strftime('%Y-%m-%d')
                    except:
                        pass
                
                title = entry.get('title', '')
                source = 'Google News'
                if ' - ' in title:
                    parts = title.rsplit(' - ', 1)
                    if len(parts) == 2:
                        title = parts[0]
                        source = parts[1]
                
                summary = entry.get('summary', title)
                if summary:
                    summary = BeautifulSoup(summary, 'html.parser').get_text()[:1000]
                
                articles.append({
                    'symbol': symbol,
                    'company_name': company_name,
                    'sector': symbol_to_sector.get(symbol, 'Unknown'),
                    'doc_type': 'news',
                    'title': title,
                    'content': summary,
                    'source': source,
                    'url': entry.get('link', ''),
                    'published_date': pub_date_str,
                    'ingestion_timestamp': datetime.now()
                })
            except:
                continue
    except:
        pass
    
    return articles

# Test
print("Testing Google News...")
test = fetch_google_news("COST", "Costco Wholesale", 3)
print(f"  ✅ Found {len(test)} articles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📄 Source 3: SEC EDGAR Filings (10-K, 10-Q, 8-K)

# COMMAND ----------

# CIK mapping for major companies
SEC_CIK_MAP = {
    "AAPL": "0000320193", "MSFT": "0000789019", "GOOGL": "0001652044",
    "AMZN": "0001018724", "META": "0001326801", "NVDA": "0001045810",
    "TSLA": "0001318605", "JPM": "0000019617", "BAC": "0000070858",
    "WMT": "0000104169", "COST": "0000909832", "HD": "0000354950",
    "DIS": "0001744489", "NFLX": "0001065280", "PFE": "0000078003",
    "JNJ": "0000200406", "UNH": "0000731766", "V": "0001403161",
    "MA": "0001141391", "XOM": "0000034088", "CVX": "0000093410",
    "BA": "0000012927", "CAT": "0000018230", "GS": "0000886982",
    "MS": "0000895421", "INTC": "0000050863", "AMD": "0000002488",
    "CRM": "0001108524", "ADBE": "0000796343", "ORCL": "0001341439",
    "CSCO": "0000858877", "IBM": "0000051143", "QCOM": "0000804328",
}

def extract_section(text, start_patterns, end_patterns, max_len=2000):
    """Extract a section of text between start/end patterns."""
    if not text:
        return ""
    lower = text.lower()
    start_idx = -1
    for pat in start_patterns:
        idx = lower.find(pat)
        if idx != -1:
            start_idx = idx
            break
    if start_idx == -1:
        return ""

    end_idx = len(text)
    for pat in end_patterns:
        idx = lower.find(pat, start_idx + 5)
        if idx != -1:
            end_idx = min(end_idx, idx)

    section = text[start_idx:end_idx]
    section = re.sub(r'\s+', ' ', section).strip()
    return section[:max_len]

def fetch_sec_filings(symbol, filing_types=['10-K', '10-Q', '8-K'], max_per_type=3):
    """
    Fetch SEC filings with actual content excerpts
    10-K: Annual report (business overview, risk factors)
    10-Q: Quarterly report (financial updates)
    8-K: Material events (earnings, acquisitions, leadership)
    """
    articles = []
    cik = SEC_CIK_MAP.get(symbol)
    if not cik:
        return articles
    
    try:
        headers = {
            'User-Agent': 'RiskBricks Research research@riskbricks.com',
            'Accept': 'application/json'
        }
        
        # Get company filings
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return articles
        
        data = response.json()
        company_name = data.get('name', symbol_to_company.get(symbol, symbol))
        
        filings = data.get('filings', {}).get('recent', {})
        forms = filings.get('form', [])
        dates = filings.get('filingDate', [])
        accessions = filings.get('accessionNumber', [])
        descriptions = filings.get('primaryDocDescription', [])
        primary_docs = filings.get('primaryDocument', [])
        
        # Track counts per type
        type_counts = {t: 0 for t in filing_types}
        
        for i, form in enumerate(forms):
            if form in filing_types and type_counts[form] < max_per_type:
                filing_date = dates[i] if i < len(dates) else datetime.now().strftime('%Y-%m-%d')
                accession = accessions[i].replace('-', '') if i < len(accessions) else ''
                desc = descriptions[i] if i < len(descriptions) else ''
                primary_doc = primary_docs[i] if i < len(primary_docs) else ''

                # Fetch full filing text (best effort)
                filing_text = ""
                if accession and primary_doc:
                    try:
                        cik_int = str(int(cik))
                        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}/{primary_doc}"
                        filing_resp = requests.get(filing_url, headers=headers, timeout=20)
                        if filing_resp.status_code == 200:
                            soup = BeautifulSoup(filing_resp.text, "lxml")
                            filing_text = soup.get_text(separator="\n")
                            filing_text = re.sub(r'\n{2,}', '\n', filing_text)
                    except:
                        filing_text = ""
                
                # Create meaningful content based on filing type
                if form == '10-K':
                    title = f"{company_name} Annual Report (10-K) - Fiscal Year {filing_date[:4]}"
                    # Extract key sections if available
                    risk_factors = extract_section(
                        filing_text,
                        start_patterns=["item 1a", "risk factors"],
                        end_patterns=["item 1b", "item 2", "unresolved staff comments"]
                    )
                    mdna = extract_section(
                        filing_text,
                        start_patterns=["item 7", "management's discussion and analysis", "md&a"],
                        end_patterns=["item 7a", "quantitative and qualitative", "item 8"]
                    )

                    content = f"""SEC 10-K Annual Report for {company_name} filed on {filing_date}.

Key sections extracted:

RISK FACTORS:
{risk_factors if risk_factors else 'Not available in extracted text.'}

MD&A (Management Discussion & Analysis):
{mdna if mdna else 'Not available in extracted text.'}
"""
                
                elif form == '10-Q':
                    title = f"{company_name} Quarterly Report (10-Q) - {filing_date}"
                    mdna = extract_section(
                        filing_text,
                        start_patterns=["item 2", "management's discussion and analysis", "md&a"],
                        end_patterns=["item 3", "quantitative and qualitative", "item 4"]
                    )
                    risk_factors = extract_section(
                        filing_text,
                        start_patterns=["item 1a", "risk factors"],
                        end_patterns=["item 2", "item 3"]
                    )

                    content = f"""SEC 10-Q Quarterly Report for {company_name} filed on {filing_date}.

Key sections extracted:

MD&A (Management Discussion & Analysis):
{mdna if mdna else 'Not available in extracted text.'}

RISK FACTORS:
{risk_factors if risk_factors else 'Not available in extracted text.'}
"""
                
                else:  # 8-K
                    title = f"{company_name} Material Event (8-K) - {filing_date}"
                    content = f"""SEC 8-K Material Event Report for {company_name} filed on {filing_date}.

This filing indicates a material event occurred at {company_name}. See SEC link for full details."""
                
                articles.append({
                    'symbol': symbol,
                    'company_name': company_name,
                    'sector': symbol_to_sector.get(symbol, 'Unknown'),
                    'doc_type': f'sec_{form.lower().replace("-", "")}',
                    'title': title,
                    'content': content,
                    'source': 'SEC EDGAR',
                    'url': f"https://www.sec.gov/Archives/edgar/data/{str(int(cik))}/{accession}/{primary_doc}" if primary_doc else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}",
                    'published_date': filing_date,
                    'ingestion_timestamp': datetime.now()
                })
                type_counts[form] += 1
    
    except Exception as e:
        pass
    
    return articles

def _strip_ns(tag):
    return tag.split('}')[-1] if '}' in tag else tag

def fetch_form4_filings(symbol, max_forms=2):
    """
    Fetch Form 4 insider trades (free, historical).
    Parses XML to extract basic transaction details.
    """
    articles = []
    cik = SEC_CIK_MAP.get(symbol)
    if not cik:
        return articles

    try:
        headers = {
            'User-Agent': 'RiskBricks Research research@riskbricks.com',
            'Accept': 'application/json'
        }

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return articles

        data = response.json()
        company_name = data.get('name', symbol_to_company.get(symbol, symbol))

        filings = data.get('filings', {}).get('recent', {})
        forms = filings.get('form', [])
        dates = filings.get('filingDate', [])
        accessions = filings.get('accessionNumber', [])
        primary_docs = filings.get('primaryDocument', [])

        count = 0
        for i, form in enumerate(forms):
            if form not in ['4', '4/A'] or count >= max_forms:
                continue

            filing_date = dates[i] if i < len(dates) else datetime.now().strftime('%Y-%m-%d')
            accession = accessions[i].replace('-', '') if i < len(accessions) else ''
            primary_doc = primary_docs[i] if i < len(primary_docs) else ''

            cik_int = str(int(cik))
            filing_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}"

            # Find XML document
            xml_url = ""
            try:
                idx_resp = requests.get(f"{filing_base}/index.json", headers=headers, timeout=20)
                if idx_resp.status_code == 200:
                    files = idx_resp.json().get('directory', {}).get('item', [])
                    xml_files = [f['name'] for f in files if f['name'].lower().endswith('.xml')]
                    if primary_doc.lower().endswith('.xml'):
                        xml_url = f"{filing_base}/{primary_doc}"
                    elif xml_files:
                        xml_url = f"{filing_base}/{xml_files[0]}"
            except:
                xml_url = ""

            content = f"Form 4 insider trading report for {company_name} filed on {filing_date}."
            if xml_url:
                try:
                    xml_resp = requests.get(xml_url, headers=headers, timeout=20)
                    if xml_resp.status_code == 200:
                        root = ET.fromstring(xml_resp.text)
                        issuer = ""
                        owner = ""
                        transactions = []

                        for el in root.iter():
                            tag = _strip_ns(el.tag)
                            if tag == "issuerName":
                                issuer = el.text or issuer
                            elif tag == "rptOwnerName":
                                owner = el.text or owner

                        # Parse non-derivative transactions (basic)
                        for txn in root.iter():
                            if _strip_ns(txn.tag) == "nonDerivativeTransaction":
                                txn_data = {"date": "", "code": "", "shares": "", "price": ""}
                                for child in txn.iter():
                                    ctag = _strip_ns(child.tag)
                                    if ctag == "transactionDate":
                                        for g in child.iter():
                                            if _strip_ns(g.tag) == "value":
                                                txn_data["date"] = g.text or ""
                                    elif ctag == "transactionCode":
                                        txn_data["code"] = child.text or ""
                                    elif ctag == "transactionShares":
                                        for g in child.iter():
                                            if _strip_ns(g.tag) == "value":
                                                txn_data["shares"] = g.text or ""
                                    elif ctag == "transactionPricePerShare":
                                        for g in child.iter():
                                            if _strip_ns(g.tag) == "value":
                                                txn_data["price"] = g.text or ""
                                transactions.append(txn_data)

                        content = f"""Form 4 insider trading report for {company_name} filed on {filing_date}.

Reporting Owner: {owner or 'Unknown'}
Issuer: {issuer or company_name}

Transactions:
{'; '.join([f"{t['date']} | Code {t['code']} | Shares {t['shares']} | Price {t['price']}" for t in transactions[:3]]) or 'No transaction details parsed.'}
"""
                except Exception:
                    pass

            articles.append({
                'symbol': symbol,
                'company_name': company_name,
                'sector': symbol_to_sector.get(symbol, 'Unknown'),
                'doc_type': 'sec_form4',
                'title': f"{company_name} Form 4 Insider Trade - {filing_date}",
                'content': content,
                'source': 'SEC EDGAR',
                'url': xml_url if xml_url else f"{filing_base}/index.json",
                'published_date': filing_date,
                'ingestion_timestamp': datetime.now()
            })
            count += 1

    except Exception:
        pass

    return articles

# Test
print("Testing SEC EDGAR...")
test = fetch_sec_filings("COST", max_per_type=2)
print(f"  ✅ Found {len(test)} filings")
for t in test[:2]:
    print(f"     - {t['title'][:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Source 4: Wikipedia (Company Background)

# COMMAND ----------

def fetch_wikipedia_info(symbol, company_name):
    """Fetch company background from Wikipedia"""
    articles = []
    
    try:
        wiki = wikipediaapi.Wikipedia(
            user_agent='RiskBricks/1.0 (research@riskbricks.com)',
            language='en'
        )
        
        # Try to find the company page
        search_terms = [
            company_name,
            f"{company_name} (company)",
            f"{company_name} Corporation",
            f"{company_name} Inc"
        ]
        
        page = None
        for term in search_terms:
            p = wiki.page(term)
            if p.exists():
                page = p
                break
        
        if page and page.exists():
            # Get summary (first few paragraphs)
            summary = page.summary[:3000] if len(page.summary) > 3000 else page.summary
            
            articles.append({
                'symbol': symbol,
                'company_name': company_name,
                'sector': symbol_to_sector.get(symbol, 'Unknown'),
                'doc_type': 'wiki_company',
                'title': f"{company_name} - Company Overview",
                'content': summary,
                'source': 'Wikipedia',
                'url': page.fullurl,
                'published_date': datetime.now().strftime('%Y-%m-%d'),
                'ingestion_timestamp': datetime.now()
            })
    
    except Exception as e:
        pass
    
    return articles

# Test
print("Testing Wikipedia...")
test = fetch_wikipedia_info("COST", "Costco")
print(f"  ✅ Found {len(test)} articles")
if test:
    print(f"     - {test[0]['content'][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎙️ Source 5: Earnings Call Transcripts (Simulated from SEC 8-K)

# COMMAND ----------

def fetch_earnings_info(symbol, company_name):
    """
    Create earnings-related content from recent quarterly data
    Note: Full transcripts require paid APIs (SeekingAlpha, etc.)
    This creates structured earnings information from available data
    """
    articles = []
    
    try:
        # Get recent stock data to estimate earnings periods
        stock_data = spark.sql(f"""
            SELECT 
                date,
                close,
                volume
            FROM riskbricks.silver.stock_prices
            WHERE symbol = '{symbol}'
            ORDER BY date DESC
            LIMIT 90
        """).collect()
        
        if stock_data:
            # Get quarterly periods
            latest_date = stock_data[0]['date']
            latest_price = stock_data[0]['close']
            
            # Find high volume days (potential earnings)
            avg_volume = sum(r['volume'] for r in stock_data) / len(stock_data)
            high_volume_days = [r for r in stock_data if r['volume'] > avg_volume * 2]
            
            for hvd in high_volume_days[:2]:  # Last 2 high-volume events
                articles.append({
                    'symbol': symbol,
                    'company_name': company_name,
                    'sector': symbol_to_sector.get(symbol, 'Unknown'),
                    'doc_type': 'earnings_event',
                    'title': f"{company_name} Significant Trading Activity - {hvd['date']}",
                    'content': f"""Significant trading activity detected for {company_name} ({symbol}) on {hvd['date']}.

Volume: {hvd['volume']:,.0f} shares (significantly above average)
Closing Price: ${hvd['close']:.2f}

High volume trading days often correspond to:
- Earnings announcements
- Analyst upgrades/downgrades
- M&A news
- Product launches
- Regulatory updates

For detailed earnings call transcripts, refer to SEC 8-K filings or investor relations websites.""",
                    'source': 'Trading Data Analysis',
                    'url': f"https://finance.yahoo.com/quote/{symbol}",
                    'published_date': str(hvd['date']),
                    'ingestion_timestamp': datetime.now()
                })
    
    except Exception as e:
        pass
    
    return articles

# Test
print("Testing Earnings Events...")
test = fetch_earnings_info("COST", "Costco Wholesale")
print(f"  ✅ Found {len(test)} events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Source 6: Stock Price Context

# COMMAND ----------

def fetch_stock_context(symbol, company_name):
    """Create stock price context for RAG queries"""
    articles = []
    
    try:
        # Get recent price data
        data = spark.sql(f"""
            SELECT 
                symbol,
                MAX(date) as latest_date,
                FIRST(close) as latest_close,
                AVG(close) as avg_close_30d,
                MIN(close) as min_close_30d,
                MAX(close) as max_close_30d,
                AVG(volume) as avg_volume
            FROM riskbricks.silver.stock_prices
            WHERE symbol = '{symbol}'
              AND date >= DATE_SUB(CURRENT_DATE(), 30)
            GROUP BY symbol
        """).collect()
        
        if data and len(data) > 0:
            d = data[0]
            
            articles.append({
                'symbol': symbol,
                'company_name': company_name,
                'sector': symbol_to_sector.get(symbol, 'Unknown'),
                'doc_type': 'stock_context',
                'title': f"{company_name} ({symbol}) - Current Stock Overview",
                'content': f"""{company_name} ({symbol}) Stock Overview

Current Price: ${d['latest_close']:.2f} (as of {d['latest_date']})
30-Day Average: ${d['avg_close_30d']:.2f}
30-Day Range: ${d['min_close_30d']:.2f} - ${d['max_close_30d']:.2f}
Average Daily Volume: {d['avg_volume']:,.0f} shares

This stock is part of the {symbol_to_sector.get(symbol, 'Unknown')} sector.""",
                'source': 'RiskBricks Analytics',
                'url': f"https://finance.yahoo.com/quote/{symbol}",
                'published_date': str(d['latest_date']),
                'ingestion_timestamp': datetime.now()
            })
    
    except Exception as e:
        pass
    
    return articles

# Test
print("Testing Stock Context...")
test = fetch_stock_context("COST", "Costco Wholesale")
print(f"  ✅ Found {len(test)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Fetch All Data Sources

# COMMAND ----------

print("=" * 70)
print("📰 BUILDING COMPREHENSIVE RAG CORPUS")
print("=" * 70)
print("")

all_articles = []
processed = 0
errors = 0

# Process ALL stocks in the company universe
symbols_to_process = portfolio_symbols  # All stocks (400+)
print(f"📊 Processing ALL {len(symbols_to_process)} stocks...")

for i, symbol in enumerate(symbols_to_process):
    company_name = symbol_to_company.get(symbol, symbol)
    symbol_articles = []
    
    try:
        # 1. Yahoo News
        yahoo = fetch_yahoo_news(symbol, max_articles=5)
        symbol_articles.extend(yahoo)
        
        # 2. Google News
        google = fetch_google_news(symbol, company_name, max_articles=3)
        symbol_articles.extend(google)
        
        # 3. SEC Filings (for major stocks)
        if symbol in SEC_CIK_MAP:
            sec = fetch_sec_filings(symbol, max_per_type=2)
            symbol_articles.extend(sec)

        # 3b. SEC Form 4 Insider Trades
        if symbol in SEC_CIK_MAP:
            form4 = fetch_form4_filings(symbol, max_forms=2)
            symbol_articles.extend(form4)
        
        # 4. Wikipedia (for all stocks)
        wiki = fetch_wikipedia_info(symbol, company_name)
        symbol_articles.extend(wiki)
        
        # 5. Stock Context
        stock_ctx = fetch_stock_context(symbol, company_name)
        symbol_articles.extend(stock_ctx)
        
        # 6. Earnings Events
        earnings = fetch_earnings_info(symbol, company_name)
        symbol_articles.extend(earnings)
        
        # Rate limiting
        time.sleep(0.3)
        
        if symbol_articles:
            all_articles.extend(symbol_articles)
            processed += 1
        
        # Progress
        if (i + 1) % 20 == 0:
            print(f"  📊 Processed {i+1}/{len(symbols_to_process)} stocks, {len(all_articles)} total documents")
    
    except Exception as e:
        errors += 1

print("")
print(f"✅ Completed: {processed} stocks, {len(all_articles)} documents, {errors} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Bronze Layer

# COMMAND ----------

if len(all_articles) == 0:
    print("⚠️ No articles collected. Check network access.")
    dbutils.notebook.exit("No articles to save")

# Convert to DataFrame
articles_df = pd.DataFrame(all_articles)

# Create unique document ID
articles_df['doc_id'] = articles_df.apply(
    lambda row: hashlib.md5(
        f"{row['symbol']}{row['title']}{row['published_date']}{row['doc_type']}".encode()
    ).hexdigest()[:16],
    axis=1
)

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(articles_df)

# Remove duplicates
spark_df = spark_df.dropDuplicates(['doc_id'])

# Show sample
print("📰 Sample documents:")
spark_df.select("doc_type", "symbol", "title", "source").show(15, truncate=50)

# COMMAND ----------

# Show document type distribution
print("📊 Document Type Distribution:")
spark_df.groupBy("doc_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Save to Bronze table (append/merge, no full overwrite)
table_name = f"{catalog}.bronze.rag_corpus"

if not spark.catalog.tableExists(table_name):
    spark_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
else:
    spark_df.createOrReplaceTempView("rag_updates")
    spark.sql(f"""
        MERGE INTO {table_name} AS t
        USING rag_updates AS s
        ON t.doc_id = s.doc_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📜 Integrate GDELT Historical Data
# MAGIC 
# MAGIC Merge historical news events from GDELT (2015-2024) into the RAG corpus.

# COMMAND ----------

# Check if GDELT historical data exists and merge it
try:
    gdelt_count = spark.sql("SELECT COUNT(*) FROM riskbricks.bronze.historical_news_gdelt").collect()[0][0]
    print(f"📜 Found {gdelt_count:,} GDELT historical events")
    
    if gdelt_count > 0:
        # Transform GDELT data to RAG corpus format
        gdelt_docs = spark.sql("""
            SELECT 
                CONCAT('gdelt_', event_id) as doc_id,
                symbol,
                COALESCE(company_name, symbol) as company_name,
                COALESCE(sector, 'Unknown') as sector,
                'historical_news' as doc_type,
                CONCAT('GDELT Event: ', COALESCE(actor1_name, 'Unknown'), ' - ', symbol) as title,
                CONCAT(
                    'Historical news event on ', event_date, '. ',
                    'Actors: ', COALESCE(actor1_name, 'Unknown'), ' and ', COALESCE(actor2_name, 'Unknown'), '. ',
                    'Sentiment: ', ROUND(avg_tone, 2), '. ',
                    'Mentions: ', num_mentions, '. ',
                    'Source: ', COALESCE(source_url, 'GDELT Database')
                ) as content,
                'GDELT Historical' as source,
                COALESCE(source_url, '') as url,
                CAST(event_date AS STRING) as published_date,
                CURRENT_TIMESTAMP() as ingestion_timestamp
            FROM riskbricks.bronze.historical_news_gdelt
            WHERE symbol IS NOT NULL
        """)
        
        gdelt_doc_count = gdelt_docs.count()
        print(f"   Transformed {gdelt_doc_count:,} GDELT documents")
        
        # Append to RAG corpus
        gdelt_docs.write.mode("append").saveAsTable(table_name)
        print(f"✅ Added {gdelt_doc_count:,} historical documents to RAG corpus")
    
    # Merge GDELT GKG (text-like metadata)
    try:
        gkg_count = spark.sql("SELECT COUNT(*) FROM riskbricks.bronze.historical_news_gdelt_gkg").collect()[0][0]
        print(f"📜 Found {gkg_count:,} GDELT GKG records")
        
        if gkg_count > 0:
            gkg_docs = spark.sql("""
                SELECT 
                    CONCAT('gkg_', gkg_record_id) as doc_id,
                    symbol,
                    COALESCE(company_name, symbol) as company_name,
                    COALESCE(sector, 'Unknown') as sector,
                    'historical_news_gkg' as doc_type,
                    CONCAT('GKG: ', source_common_name) as title,
                    CONCAT(
                        'GDELT GKG metadata for ', symbol, '. ',
                        'Source: ', COALESCE(source_common_name, 'Unknown'), '. ',
                        'Themes: ', COALESCE(themes, ''), '. ',
                        'Organizations: ', COALESCE(organizations, ''), '. ',
                        'Persons: ', COALESCE(persons, ''), '. ',
                        'Tone: ', COALESCE(tone, '')
                    ) as content,
                    'GDELT GKG' as source,
                    COALESCE(document_identifier, '') as url,
                    CAST(event_date AS STRING) as published_date,
                    CURRENT_TIMESTAMP() as ingestion_timestamp
                FROM riskbricks.bronze.historical_news_gdelt_gkg
                WHERE symbol IS NOT NULL
            """)
            
            gkg_doc_count = gkg_docs.count()
            print(f"   Transformed {gkg_doc_count:,} GKG documents")
            gkg_docs.write.mode("append").saveAsTable(table_name)
            print(f"✅ Added {gkg_doc_count:,} GKG documents to RAG corpus")
    except Exception as e:
        print(f"⚠️ GDELT GKG integration skipped: {str(e)}")
        print("   Run notebooks/ingestion/gdelt/bronze_ingest_gdelt.py (GKG required)")
        
except Exception as e:
    print(f"⚠️ GDELT integration skipped: {str(e)}")
    print("   Run notebooks/ingestion/gdelt/bronze_ingest_gdelt.py first for historical coverage")

spark.sql(f"COMMENT ON TABLE {table_name} IS 'Comprehensive RAG corpus: news, filings, wiki, stock data'")

total_records = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
unique_stocks = spark.sql(f"SELECT COUNT(DISTINCT symbol) as count FROM {table_name}").collect()[0]['count']
doc_types = spark.sql(f"SELECT COUNT(DISTINCT doc_type) as count FROM {table_name}").collect()[0]['count']

print(f"""
✅ Saved to {table_name}
   Total documents: {total_records}
   Unique stocks: {unique_stocks}
   Document types: {doc_types}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Corpus Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Documents by type
# MAGIC SELECT 
# MAGIC   doc_type,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   COUNT(DISTINCT symbol) as unique_stocks,
# MAGIC   COUNT(DISTINCT source) as unique_sources
# MAGIC FROM riskbricks.bronze.rag_corpus
# MAGIC GROUP BY doc_type
# MAGIC ORDER BY doc_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top stocks by coverage
# MAGIC SELECT 
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   COUNT(*) as doc_count,
# MAGIC   COUNT(DISTINCT doc_type) as doc_types
# MAGIC FROM riskbricks.bronze.rag_corpus
# MAGIC GROUP BY symbol, company_name
# MAGIC ORDER BY doc_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Costco documents
# MAGIC SELECT 
# MAGIC   doc_type,
# MAGIC   title,
# MAGIC   source,
# MAGIC   published_date
# MAGIC FROM riskbricks.bronze.rag_corpus
# MAGIC WHERE symbol = 'COST'
# MAGIC ORDER BY doc_type, published_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Comprehensive Corpus Complete!
# MAGIC 
# MAGIC **Next Step**: Run `02_create_vector_index.py` to create embeddings

# COMMAND ----------

print(f"""
================================================================================
✅ COMPREHENSIVE RAG CORPUS COMPLETE!
================================================================================

📊 Data Sources Included:
   ┌─────────────────────┬───────────────────────────────────────┐
   │ Source              │ Description                           │
   ├─────────────────────┼───────────────────────────────────────┤
   │ Yahoo Finance       │ Real-time stock news headlines        │
   │ Google News         │ Broader financial news coverage       │
   │ SEC EDGAR 10-K      │ Annual reports (business, risks)      │
   │ SEC EDGAR 10-Q      │ Quarterly financial updates           │
   │ SEC EDGAR 8-K       │ Material events (earnings, M&A)       │
   │ Wikipedia           │ Company background and history        │
   │ Stock Context       │ Current price and trading data        │
   │ Earnings Events     │ High-volume trading day analysis      │
   └─────────────────────┴───────────────────────────────────────┘

📈 Statistics:
   - Total documents: {total_records}
   - Unique stocks: {unique_stocks}
   - Document types: {doc_types}
   - Table: riskbricks.bronze.rag_corpus

🎯 What This Enables:
   - "What's in Costco's annual report?" → 10-K content
   - "Tell me about Apple's history" → Wikipedia
   - "What happened to Tesla today?" → News
   - "Any SEC filings for Microsoft?" → 10-K, 10-Q, 8-K

🔄 Next Steps:
   1. Run 02_create_vector_index.py - Create embeddings
   2. Run 03_news_rag_agent.py - Build RAG agent
   3. Query: "What's in Amazon's latest 10-K?"

================================================================================
""")

dbutils.notebook.exit(json.dumps({
    'status': 'success',
    'documents': total_records,
    'stocks': unique_stocks,
    'doc_types': doc_types
}))
