# Databricks notebook source
# MAGIC %md
# MAGIC # 📚 Periodic Docs Loader (SEC, Wikipedia, Earnings, Stock Context, GDELT)
# MAGIC
# MAGIC **Purpose**: Periodic ingestion of slow-changing sources for RAG
# MAGIC
# MAGIC **Sources**:
# MAGIC - SEC EDGAR (10-K, 10-Q, 8-K, Form 4)
# MAGIC - Wikipedia
# MAGIC - Earnings events (from trading data)
# MAGIC - Stock context (30-day stats)
# MAGIC - GDELT historical events + GKG (append/merge)
# MAGIC
# MAGIC **Output**: `riskbricks.bronze.rag_corpus` (MERGE by `doc_id`)

# COMMAND ----------

# Widgets (run this cell first)
dbutils.widgets.text("max_sec_per_type", "2", "Max SEC filings per type")
dbutils.widgets.text("max_form4", "2", "Max Form 4 filings")
dbutils.widgets.text("sleep_seconds", "0.3", "Sleep between symbols (seconds)")

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
from datetime import datetime
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
from bs4 import BeautifulSoup
import hashlib
import time
import wikipediaapi

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

max_sec_per_type = int(dbutils.widgets.get("max_sec_per_type"))
max_form4 = int(dbutils.widgets.get("max_form4"))
sleep_seconds = float(dbutils.widgets.get("sleep_seconds"))

print(f"✅ Using catalog: {catalog}")
print(f"✅ SEC max: {max_sec_per_type}, Form4 max: {max_form4}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Get Portfolio Symbols

# COMMAND ----------

symbols_df = spark.sql("""
    SELECT DISTINCT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")

portfolio_symbols = [row.symbol for row in symbols_df.collect()]
symbol_to_company = {row.symbol: row.company_name for row in symbols_df.collect()}
symbol_to_sector = {row.symbol: row.sector for row in symbols_df.collect()}

print(f"📊 Periodic ingestion for {len(portfolio_symbols)} stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📄 SEC EDGAR (10-K, 10-Q, 8-K)

# COMMAND ----------

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
    section = re.sub(r"\s+", " ", section).strip()
    return section[:max_len]

def fetch_sec_filings(symbol, filing_types=["10-K", "10-Q", "8-K"], max_per_type=2):
    articles = []
    cik = SEC_CIK_MAP.get(symbol)
    if not cik:
        return articles

    try:
        headers = {
            "User-Agent": "RiskBricks Research research@riskbricks.com",
            "Accept": "application/json"
        }

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return articles

        data = response.json()
        company_name = data.get("name", symbol_to_company.get(symbol, symbol))

        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        dates = filings.get("filingDate", [])
        accessions = filings.get("accessionNumber", [])
        descriptions = filings.get("primaryDocDescription", [])
        primary_docs = filings.get("primaryDocument", [])

        type_counts = {t: 0 for t in filing_types}

        for i, form in enumerate(forms):
            if form in filing_types and type_counts[form] < max_per_type:
                filing_date = dates[i] if i < len(dates) else datetime.now().strftime("%Y-%m-%d")
                accession = accessions[i].replace("-", "") if i < len(accessions) else ""
                primary_doc = primary_docs[i] if i < len(primary_docs) else ""

                filing_text = ""
                if accession and primary_doc:
                    try:
                        cik_int = str(int(cik))
                        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}/{primary_doc}"
                        filing_resp = requests.get(filing_url, headers=headers, timeout=20)
                        if filing_resp.status_code == 200:
                            soup = BeautifulSoup(filing_resp.text, "lxml")
                            filing_text = soup.get_text(separator="\n")
                            filing_text = re.sub(r"\n{2,}", "\n", filing_text)
                    except Exception:
                        filing_text = ""

                if form == "10-K":
                    title = f"{company_name} Annual Report (10-K) - Fiscal Year {filing_date[:4]}"
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
                elif form == "10-Q":
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
                else:
                    title = f"{company_name} Material Event (8-K) - {filing_date}"
                    content = f"""SEC 8-K Material Event Report for {company_name} filed on {filing_date}.

This filing indicates a material event occurred at {company_name}. See SEC link for full details."""

                articles.append({
                    "symbol": symbol,
                    "company_name": company_name,
                    "sector": symbol_to_sector.get(symbol, "Unknown"),
                    "doc_type": f"sec_{form.lower().replace('-', '')}",
                    "title": title,
                    "content": content,
                    "source": "SEC EDGAR",
                    "url": f"https://www.sec.gov/Archives/edgar/data/{str(int(cik))}/{accession}/{primary_doc}" if primary_doc else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}",
                    "published_date": filing_date,
                    "ingestion_timestamp": datetime.now()
                })
                type_counts[form] += 1

    except Exception:
        pass

    return articles

def _strip_ns(tag):
    return tag.split("}")[-1] if "}" in tag else tag

def fetch_form4_filings(symbol, max_forms=2):
    articles = []
    cik = SEC_CIK_MAP.get(symbol)
    if not cik:
        return articles

    try:
        headers = {
            "User-Agent": "RiskBricks Research research@riskbricks.com",
            "Accept": "application/json"
        }

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return articles

        data = response.json()
        company_name = data.get("name", symbol_to_company.get(symbol, symbol))

        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        dates = filings.get("filingDate", [])
        accessions = filings.get("accessionNumber", [])
        primary_docs = filings.get("primaryDocument", [])

        count = 0
        for i, form in enumerate(forms):
            if form not in ["4", "4/A"] or count >= max_forms:
                continue

            filing_date = dates[i] if i < len(dates) else datetime.now().strftime("%Y-%m-%d")
            accession = accessions[i].replace("-", "") if i < len(accessions) else ""
            primary_doc = primary_docs[i] if i < len(primary_docs) else ""

            cik_int = str(int(cik))
            filing_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}"

            xml_url = ""
            try:
                idx_resp = requests.get(f"{filing_base}/index.json", headers=headers, timeout=20)
                if idx_resp.status_code == 200:
                    files = idx_resp.json().get("directory", {}).get("item", [])
                    xml_files = [f["name"] for f in files if f["name"].lower().endswith(".xml")]
                    if primary_doc.lower().endswith(".xml"):
                        xml_url = f"{filing_base}/{primary_doc}"
                    elif xml_files:
                        xml_url = f"{filing_base}/{xml_files[0]}"
            except Exception:
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
                "symbol": symbol,
                "company_name": company_name,
                "sector": symbol_to_sector.get(symbol, "Unknown"),
                "doc_type": "sec_form4",
                "title": f"{company_name} Form 4 Insider Trade - {filing_date}",
                "content": content,
                "source": "SEC EDGAR",
                "url": xml_url if xml_url else f"{filing_base}/index.json",
                "published_date": filing_date,
                "ingestion_timestamp": datetime.now()
            })
            count += 1

    except Exception:
        pass

    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Wikipedia

# COMMAND ----------

def fetch_wikipedia_info(symbol, company_name):
    articles = []
    try:
        wiki = wikipediaapi.Wikipedia(
            user_agent="RiskBricks/1.0 (research@riskbricks.com)",
            language="en"
        )
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
            summary = page.summary[:3000] if len(page.summary) > 3000 else page.summary
            articles.append({
                "symbol": symbol,
                "company_name": company_name,
                "sector": symbol_to_sector.get(symbol, "Unknown"),
                "doc_type": "wiki_company",
                "title": f"{company_name} - Company Overview",
                "content": summary,
                "source": "Wikipedia",
                "url": page.fullurl,
                "published_date": datetime.now().strftime("%Y-%m-%d"),
                "ingestion_timestamp": datetime.now()
            })
    except Exception:
        pass
    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎙️ Earnings Events + 📊 Stock Context

# COMMAND ----------

def fetch_earnings_info(symbol, company_name):
    articles = []
    try:
        stock_data = spark.sql(f"""
            SELECT date, close, volume
            FROM riskbricks.silver.stock_prices
            WHERE symbol = '{symbol}'
            ORDER BY date DESC
            LIMIT 90
        """).collect()

        if stock_data:
            avg_volume = sum(r["volume"] for r in stock_data) / len(stock_data)
            high_volume_days = [r for r in stock_data if r["volume"] > avg_volume * 2]

            for hvd in high_volume_days[:2]:
                articles.append({
                    "symbol": symbol,
                    "company_name": company_name,
                    "sector": symbol_to_sector.get(symbol, "Unknown"),
                    "doc_type": "earnings_event",
                    "title": f"{company_name} Significant Trading Activity - {hvd['date']}",
                    "content": f"""Significant trading activity detected for {company_name} ({symbol}) on {hvd['date']}.

Volume: {hvd['volume']:,.0f} shares (significantly above average)
Closing Price: ${hvd['close']:.2f}

High volume trading days often correspond to:
- Earnings announcements
- Analyst upgrades/downgrades
- M&A news
- Product launches
- Regulatory updates

For detailed earnings call transcripts, refer to SEC 8-K filings or investor relations websites.""",
                    "source": "Trading Data Analysis",
                    "url": f"https://finance.yahoo.com/quote/{symbol}",
                    "published_date": str(hvd["date"]),
                    "ingestion_timestamp": datetime.now()
                })
    except Exception:
        pass
    return articles

def fetch_stock_context(symbol, company_name):
    articles = []
    try:
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

        if data:
            d = data[0]
            articles.append({
                "symbol": symbol,
                "company_name": company_name,
                "sector": symbol_to_sector.get(symbol, "Unknown"),
                "doc_type": "stock_context",
                "title": f"{company_name} ({symbol}) - Current Stock Overview",
                "content": f"""{company_name} ({symbol}) Stock Overview

Current Price: ${d['latest_close']:.2f} (as of {d['latest_date']})
30-Day Average: ${d['avg_close_30d']:.2f}
30-Day Range: ${d['min_close_30d']:.2f} - ${d['max_close_30d']:.2f}
Average Daily Volume: {d['avg_volume']:,.0f} shares

This stock is part of the {symbol_to_sector.get(symbol, 'Unknown')} sector.""",
                "source": "RiskBricks Analytics",
                "url": f"https://finance.yahoo.com/quote/{symbol}",
                "published_date": str(d["latest_date"]),
                "ingestion_timestamp": datetime.now()
            })
    except Exception:
        pass
    return articles

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Fetch Periodic Sources

# COMMAND ----------

print("=" * 70)
print("📚 PERIODIC DOC INGESTION")
print("=" * 70)

all_articles = []
processed = 0
errors = 0

for i, symbol in enumerate(portfolio_symbols):
    company_name = symbol_to_company.get(symbol, symbol)
    symbol_articles = []
    try:
        if symbol in SEC_CIK_MAP:
            symbol_articles.extend(fetch_sec_filings(symbol, max_per_type=max_sec_per_type))
            symbol_articles.extend(fetch_form4_filings(symbol, max_forms=max_form4))

        symbol_articles.extend(fetch_wikipedia_info(symbol, company_name))
        symbol_articles.extend(fetch_stock_context(symbol, company_name))
        symbol_articles.extend(fetch_earnings_info(symbol, company_name))

        if symbol_articles:
            all_articles.extend(symbol_articles)
            processed += 1

        if (i + 1) % 25 == 0:
            print(f"  📊 Processed {i+1}/{len(portfolio_symbols)} stocks, {len(all_articles)} docs")

        time.sleep(sleep_seconds)
    except Exception:
        errors += 1

print("")
print(f"✅ Completed: {processed} stocks, {len(all_articles)} documents, {errors} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Bronze (MERGE by doc_id)

# COMMAND ----------

if len(all_articles) == 0:
    print("⚠️ No periodic documents collected.")
else:
    articles_df = pd.DataFrame(all_articles)
    articles_df["doc_id"] = articles_df.apply(
        lambda row: hashlib.md5(
            f"{row['symbol']}{row['title']}{row['published_date']}{row['doc_type']}".encode()
        ).hexdigest()[:16],
        axis=1
    )

    spark_df = spark.createDataFrame(articles_df).dropDuplicates(["doc_id"])

    table_name = f"{catalog}.bronze.rag_corpus"

    if not spark.catalog.tableExists(table_name):
        spark_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        spark_df.createOrReplaceTempView("rag_periodic_updates")
        spark.sql(f"""
            MERGE INTO {table_name} t
            USING rag_periodic_updates s
            ON t.doc_id = s.doc_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    total = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]["count"]
    print(f"✅ Saved periodic docs to {table_name}. Total rows: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📜 Merge GDELT Historical Data (Optional)

# COMMAND ----------

table_name = f"{catalog}.bronze.rag_corpus"

try:
    gdelt_count = spark.sql("SELECT COUNT(*) FROM riskbricks.bronze.historical_news_gdelt").collect()[0][0]
    print(f"📜 Found {gdelt_count:,} GDELT historical events")

    if gdelt_count > 0:
        gdelt_docs = spark.sql("""
            SELECT 
                CONCAT('gdelt_', event_id, '_', symbol) as doc_id,
                symbol,
                COALESCE(company_name, symbol) as company_name,
                COALESCE(sector, 'Unknown') as sector,
                'historical_news' as doc_type,
                CONCAT('GDELT Event - ', symbol) as title,
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

        gdelt_docs.createOrReplaceTempView("gdelt_docs_updates")
        spark.sql(f"""
            MERGE INTO {table_name} t
            USING gdelt_docs_updates s
            ON t.doc_id = s.doc_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("✅ Merged GDELT historical events into RAG corpus")

    gkg_count = spark.sql("SELECT COUNT(*) FROM riskbricks.bronze.historical_news_gdelt_gkg").collect()[0][0]
    print(f"📜 Found {gkg_count:,} GDELT GKG records")

    if gkg_count > 0:
        gkg_docs = spark.sql("""
            SELECT 
                CONCAT('gkg_', gkg_record_id, '_', symbol) as doc_id,
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

        gkg_docs.createOrReplaceTempView("gkg_docs_updates")
        spark.sql(f"""
            MERGE INTO {table_name} t
            USING gkg_docs_updates s
            ON t.doc_id = s.doc_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("✅ Merged GDELT GKG into RAG corpus")

except Exception:
    print("⚠️ Skipped GDELT merge (tables not available or query failed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

