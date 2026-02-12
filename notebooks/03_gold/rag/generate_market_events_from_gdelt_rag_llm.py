# Databricks notebook source
# MAGIC %md
# MAGIC # 📅 Daily Market Events (LLM Summary)
# MAGIC
# MAGIC **Purpose**: Summarize all market-impacting events for a date range using Databricks LLMs
# MAGIC
# MAGIC **Inputs**: GDELT Events, GDELT GKG, and recent news/SEC from RAG corpus
# MAGIC
# MAGIC **Output**: `riskbricks.gold.daily_market_events`

# COMMAND ----------

# Widgets (run this cell first)
dbutils.widgets.text("start_date", "2025-01-25", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2025-01-31", "End Date (YYYY-MM-DD)")
dbutils.widgets.text("model_name", "databricks-meta-llama-3-3-70b-instruct", "LLM model")

# COMMAND ----------

# MAGIC %pip install feedparser beautifulsoup4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DoubleType,
    TimestampType,
)
from datetime import datetime
import pandas as pd
import json
import hashlib
import feedparser
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
import requests
import re

catalog = "riskbricks"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
max_docs_per_day = 0  # no limit
max_chars_per_doc = 500
model_name = dbutils.widgets.get("model_name").strip()
debug = False
# Hardcoded web retrieval settings (can be exposed as widgets later)
include_web = True
max_web_articles = 200
web_queries = [
    "stock market",
    "market selloff",
    "stock rout",
    "stock crash",
    "stock plunge",
    "earnings",
    "interest rates",
    "ai stocks",
    "ai selloff",
    "chipmakers",
    "semiconductor supply chain",
    "nvidia",
    "nvda",
    "geopolitics",
    "oil prices",
    "credit spreads",
]

# Hardcoded article retrieval settings
fetch_article_text = True
max_fetch_urls = 0  # 0 = no limit
max_article_chars = 500
article_timeout_seconds = 15

if not start_date or not end_date:
    raise ValueError("Please set both start_date and end_date (YYYY-MM-DD).")

print(f"✅ Date range: {start_date} to {end_date}")
print(f"✅ Model: {model_name}")
print(f"✅ Web retrieval: {include_web}, queries={len(web_queries)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Build Evidence Corpus (per day)

# COMMAND ----------

events_df = spark.sql(f"""
    SELECT
        event_date,
        symbol,
        company_name,
        sector,
        actor1_name,
        actor2_name,
        avg_tone,
        num_articles,
        source_url
    FROM riskbricks.bronze.historical_news_gdelt
    WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
""").withColumn(
    "source_type", F.lit("gdelt_events")
).withColumn(
    "evidence_text",
    F.concat_ws(
        " | ",
        F.lit("GDELT_EVENT"),
        F.coalesce(F.col("company_name"), F.col("symbol")),
        F.coalesce(F.col("actor1_name"), F.lit("")),
        F.coalesce(F.col("actor2_name"), F.lit("")),
        F.concat(F.lit("tone="), F.col("avg_tone").cast("string")),
        F.concat(F.lit("mentions="), F.col("num_articles").cast("string")),
        F.coalesce(F.col("source_url"), F.lit(""))
    )
).withColumn(
    "score",
    F.coalesce(F.col("num_articles"), F.lit(1)).cast("double")
)

gkg_df = spark.sql(f"""
    SELECT
        event_date,
        symbol,
        company_name,
        sector,
        source_common_name,
        document_identifier,
        themes,
        organizations,
        persons,
        tone
    FROM riskbricks.bronze.historical_news_gdelt_gkg
    WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
""").withColumn(
    "source_type", F.lit("gdelt_gkg")
).withColumn(
    "evidence_text",
    F.concat_ws(
        " | ",
        F.lit("GDELT_GKG"),
        F.coalesce(F.col("company_name"), F.col("symbol")),
        F.coalesce(F.col("source_common_name"), F.lit("")),
        F.coalesce(F.col("themes"), F.lit("")),
        F.coalesce(F.col("organizations"), F.lit("")),
        F.coalesce(F.col("persons"), F.lit("")),
        F.coalesce(F.col("tone"), F.lit("")),
        F.coalesce(F.col("document_identifier"), F.lit(""))
    )
).withColumn("score", F.lit(1.0))

news_df = spark.sql(f"""
    SELECT
        to_date(published_date) as event_date,
        symbol,
        company_name,
        sector,
        title,
        content,
        source,
        url
    FROM riskbricks.bronze.rag_corpus
    WHERE to_date(published_date) BETWEEN date('{start_date}') AND date('{end_date}')
      AND doc_type IN ('news', 'sec_10k', 'sec_10q', 'sec_8k', 'sec_form4')
""").withColumn(
    "source_type", F.lit("rag_corpus")
).withColumn(
    "evidence_text",
    F.concat_ws(
        " | ",
        F.lit("RAG_NEWS"),
        F.coalesce(F.col("company_name"), F.col("symbol")),
        F.coalesce(F.col("title"), F.lit("")),
        F.substring(F.coalesce(F.col("content"), F.lit("")), 1, max_chars_per_doc),
        F.coalesce(F.col("source"), F.lit("")),
        F.coalesce(F.col("url"), F.lit(""))
    )
).withColumn("score", F.lit(1.0))

# Optional: web retrieval using GDELT 2.1 Doc API (no key)
def _gdelt_dt(dt_str, end=False):
    # dt_str format: YYYY-MM-DD
    return dt_str.replace("-", "") + ("235959" if end else "000000")

web_rows = []
if include_web and web_queries:
    start_dt = _gdelt_dt(start_date, end=False)
    end_dt = _gdelt_dt(end_date, end=True)
    collected = 0
    no_limit = max_web_articles <= 0

    for query in web_queries:
        if not no_limit and collected >= max_web_articles:
            break

        url = (
            "https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={requests.utils.quote(query)}"
            f"&mode=ArtList&format=json&maxrecords=250"
            f"&startdatetime={start_dt}&enddatetime={end_dt}"
        )
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception:
            continue

        articles = data.get("articles", [])
        for article in articles:
            if not no_limit and collected >= max_web_articles:
                break
            seen = article.get("seendate", "")
            event_date = seen[:8] if len(seen) >= 8 else start_date.replace("-", "")
            title = article.get("title", "")
            source = article.get("domain", article.get("sourcecountry", ""))
            link = article.get("url", "")
            evidence_text = f"WEB_NEWS | {title} | {source} | {link}"
            web_rows.append({
                "event_date": event_date,
                "evidence_text": evidence_text[:max_chars_per_doc],
                "score": 1.0,
                "source_type": "web_news"
            })
            collected += 1

web_schema = StructType([
    StructField("event_date", StringType(), True),
    StructField("evidence_text", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("source_type", StringType(), True),
])

if web_rows:
    web_df = spark.createDataFrame(
        [{
            "event_date": r["event_date"],
            "evidence_text": r["evidence_text"],
            "score": r["score"],
            "source_type": r["source_type"],
        } for r in web_rows],
        schema=web_schema
    ).withColumn("event_date", F.to_date("event_date", "yyyyMMdd"))
else:
    web_df = spark.createDataFrame([], schema=web_schema) \
        .withColumn("event_date", F.to_date("event_date"))

evidence_df = events_df.select("event_date", "evidence_text", "score", "source_type") \
    .unionByName(gkg_df.select("event_date", "evidence_text", "score", "source_type")) \
    .unionByName(news_df.select("event_date", "evidence_text", "score", "source_type")) \
    .unionByName(web_df.select("event_date", "evidence_text", "score", "source_type"))

evidence_ranked = evidence_df

print(f"✅ Evidence rows: {evidence_ranked.count():,}")

if debug:
    print("📊 Evidence by source:")
    evidence_df.groupBy("source_type").count().orderBy("count", ascending=False).show()
    print("📊 Evidence by day:")
    evidence_df.groupBy("event_date").count().orderBy("event_date").show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 LLM Prompt (JSON only)

# COMMAND ----------

SYSTEM_PROMPT = """You are a financial markets analyst.
Given evidence items from news/SEC/GDELT, extract ALL events that impacted markets or stocks.
Do not invent events. Use only evidence provided. If impact is uncertain, include it and set impact_level = "uncertain".
Return JSON ONLY with this schema:
[
  {
    "event_date": "YYYY-MM-DD",
    "headline": "short headline",
    "impact_summary": "why it mattered for markets or stocks",
    "symbols": ["TICKER", ...],
    "sectors": ["Sector", ...],
    "sentiment": -1.0 to 1.0,
    "impact_level": "high|medium|low|uncertain",
    "source_urls": ["url1", "url2", ...]
  }
]
Include macro, rates, FX, commodities, crypto, sector rotations, earnings, M&A, regulation, geopolitics, and supply chain issues.
If no impactful events are found, return [].
"""

# COMMAND ----------

def call_llm_json(prompt):
    row = spark.sql(
        f"SELECT ai_query('{model_name}', {json.dumps(prompt)}) as response"
    ).collect()[0]
    return row["response"]

def build_prompt(date_str, evidence_items):
    evidence_text = "\n".join([f"- {e[:max_chars_per_doc]}" for e in evidence_items])
    return f"""{SYSTEM_PROMPT}

DATE: {date_str}

EVIDENCE:
{evidence_text}
"""

_article_cache = {}

def _extract_urls(text):
    return re.findall(r"https?://\\S+", text or "")

def _fetch_article_text(url):
    if url in _article_cache:
        return _article_cache[url]
    try:
        resp = requests.get(url, timeout=article_timeout_seconds)
        if resp.status_code != 200:
            _article_cache[url] = ""
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = " ".join(soup.get_text(separator=" ").split())
        text = text[:max_article_chars]
    except Exception:
        text = ""
    _article_cache[url] = text
    return text

def enrich_evidence_with_articles(evidence_items):
    if not fetch_article_text:
        return evidence_items
    enriched = []
    fetched = 0
    no_limit = max_fetch_urls <= 0
    for item in evidence_items:
        urls = _extract_urls(item)
        for url in urls:
            if not no_limit and fetched >= max_fetch_urls:
                break
            article_text = _fetch_article_text(url)
            if article_text:
                enriched.append(f"{item} | ARTICLE: {article_text}")
            fetched += 1
        if not urls:
            enriched.append(item)
    return enriched

def chunk_evidence(evidence_items, max_chars=200000):
    """Split evidence into chunks to stay under API request size limits."""
    chunks = []
    current = []
    current_len = 0
    for item in evidence_items:
        item_txt = f"- {item[:max_chars_per_doc]}"
        item_len = len(item_txt) + 1
        if current and (current_len + item_len) > max_chars:
            chunks.append(current)
            current = []
            current_len = 0
        current.append(item)
        current_len += item_len
    if current:
        chunks.append(current)
    return chunks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Generate Daily Events Table

# COMMAND ----------

evidence_pd = evidence_ranked.groupBy("event_date") \
    .agg(F.collect_list("evidence_text").alias("evidence_items")) \
    .orderBy("event_date") \
    .toPandas()

def _to_str_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str):
        return [value] if value else []
    return [str(value)]

rows = []
for _, r in evidence_pd.iterrows():
    date_str = str(r["event_date"])
    evidence_items = r["evidence_items"] if r["evidence_items"] is not None else []
    evidence_items = list(evidence_items) if not isinstance(evidence_items, list) else evidence_items
    evidence_items = enrich_evidence_with_articles(evidence_items)
    parsed_all = []
    for chunk in chunk_evidence(evidence_items):
        prompt = build_prompt(date_str, chunk)
        response = call_llm_json(prompt)

        try:
            parsed = json.loads(response)
        except Exception:
            parsed = []
            # Try to recover JSON array from mixed output
            if isinstance(response, str):
                start = response.find("[")
                end = response.rfind("]")
                if start != -1 and end != -1 and end > start:
                    try:
                        parsed = json.loads(response[start:end + 1])
                    except Exception:
                        parsed = []

        if debug:
            print(f"🧾 {date_str} LLM raw response (truncated):")
            print((response or "")[:1500])

        parsed_all.extend(parsed)

    for item in parsed_all:
        headline = item.get("headline", "").strip()
        doc_id = hashlib.md5(f"{date_str}{headline}".encode()).hexdigest()[:16]
        rows.append({
            "doc_id": doc_id,
            "event_date": date_str,
            "headline": headline,
            "impact_summary": item.get("impact_summary", ""),
            "symbols": _to_str_list(item.get("symbols")),
            "sectors": _to_str_list(item.get("sectors")),
            "sentiment": float(item.get("sentiment", 0.0)) if str(item.get("sentiment", "")).strip() else 0.0,
            "impact_level": item.get("impact_level", "uncertain"),
            "source_urls": _to_str_list(item.get("source_urls")),
            "model": model_name,
            "created_at": datetime.now(),
            "raw_response": response
        })

if not rows:
    print("⚠️ No events returned by LLM.")
    dbutils.notebook.exit("No events to save")

schema = StructType([
    StructField("doc_id", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("impact_summary", StringType(), True),
    StructField("symbols", ArrayType(StringType()), True),
    StructField("sectors", ArrayType(StringType()), True),
    StructField("sentiment", DoubleType(), True),
    StructField("impact_level", StringType(), True),
    StructField("source_urls", ArrayType(StringType()), True),
    StructField("model", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("raw_response", StringType(), True),
])

events_df = spark.createDataFrame(rows, schema=schema) \
    .withColumn("event_date", F.to_date("event_date"))

# Expand to per-symbol rows and attach company + price context
events_expanded = events_df.withColumn("symbol", F.explode("symbols")).drop("symbols")

company_df = spark.sql("""
    SELECT symbol, company_name, sector
    FROM riskbricks.gold.company_universe
""")

prices_df = spark.sql("""
    SELECT symbol, date, close
    FROM riskbricks.silver.stock_prices
""")

events_alias = events_expanded.alias("e")
company_alias = company_df.alias("c")
prices_alias = prices_df.alias("p")

events_with_prices = events_alias \
    .join(company_alias, F.col("e.symbol") == F.col("c.symbol"), "left") \
    .drop(F.col("c.symbol")) \
    .join(
        prices_alias.select(
            F.col("symbol").alias("symbol_prev"),
            F.col("date").alias("date_prev"),
            F.col("close").alias("close_prev_day")
        ),
        (F.col("e.symbol") == F.col("symbol_prev")) &
        (F.col("date_prev") == F.date_sub(F.col("e.event_date"), 1)),
        "left"
    ) \
    .drop("symbol_prev", "date_prev") \
    .join(
        prices_alias.select(
            F.col("symbol").alias("symbol_now"),
            F.col("date").alias("date_now"),
            F.col("close").alias("close_event_day")
        ),
        (F.col("e.symbol") == F.col("symbol_now")) &
        (F.col("date_now") == F.col("e.event_date")),
        "left"
    ) \
    .drop("symbol_now", "date_now") \
    .join(
        prices_alias.select(
            F.col("symbol").alias("symbol_next"),
            F.col("date").alias("date_next"),
            F.col("close").alias("close_next_day")
        ),
        (F.col("e.symbol") == F.col("symbol_next")) &
        (F.col("date_next") == F.date_add(F.col("e.event_date"), 1)),
        "left"
    ) \
    .drop("symbol_next", "date_next")

table_name = f"{catalog}.gold.daily_market_events"

if not spark.catalog.tableExists(table_name):
    events_with_prices.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
else:
    existing_cols = [c.lower() for c in spark.table(table_name).columns]
    if "symbol" not in existing_cols:
        # Existing table is the old schema (event-level). Rebuild with symbol-level rows.
        events_with_prices.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    else:
        events_with_prices.createOrReplaceTempView("daily_events_updates")
        spark.sql(f"""
            MERGE INTO {table_name} t
            USING daily_events_updates s
            ON t.doc_id = s.doc_id AND t.symbol = s.symbol
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

total = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]["count"]
print(f"✅ Saved daily events to {table_name}. Total rows: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Price Validation (Prev / Event / Next Day)

# COMMAND ----------

validation = spark.sql(f"""
WITH events AS (
  SELECT event_date, explode(symbols) AS symbol
  FROM {table_name}
  WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
),
prices AS (
  SELECT symbol, date, close
  FROM riskbricks.silver.stock_prices
)
SELECT
  e.symbol,
  e.event_date,
  p_prev.close AS close_prev_day,
  p_now.close  AS close_event_day,
  p_next.close AS close_next_day
FROM events e
LEFT JOIN prices p_prev
  ON p_prev.symbol = e.symbol AND p_prev.date = date_sub(e.event_date, 1)
LEFT JOIN prices p_now
  ON p_now.symbol = e.symbol AND p_now.date = e.event_date
LEFT JOIN prices p_next
  ON p_next.symbol = e.symbol AND p_next.date = date_add(e.event_date, 1)
ORDER BY e.event_date, e.symbol
""")
display(validation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validation Checks

# COMMAND ----------

print("📊 Daily events by date:")
spark.sql(f"""
    SELECT event_date, COUNT(*) AS events
    FROM {table_name}
    WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
    GROUP BY event_date
    ORDER BY event_date
""").show(100, truncate=False)

print("📊 Impact level distribution:")
spark.sql(f"""
    SELECT impact_level, COUNT(*) AS events
    FROM {table_name}
    WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
    GROUP BY impact_level
    ORDER BY events DESC
""").show(truncate=False)

print("📊 Missing/empty headline or summary:")
spark.sql(f"""
    SELECT COUNT(*) AS bad_rows
    FROM {table_name}
    WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
      AND (headline IS NULL OR TRIM(headline) = '' OR impact_summary IS NULL OR TRIM(impact_summary) = '')
""").show()

print("📊 Duplicate doc_id check:")
spark.sql(f"""
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT doc_id) AS distinct_doc_ids
    FROM {table_name}
    WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Price Validation (Prev / Event / Next Day)

# COMMAND ----------

validation = spark.sql(f"""
WITH events AS (
  SELECT event_date, explode(symbols) AS symbol
  FROM {table_name}
  WHERE event_date BETWEEN date('{start_date}') AND date('{end_date}')
),
prices AS (
  SELECT symbol, date, close
  FROM riskbricks.silver.stock_prices
)
SELECT
  e.symbol,
  e.event_date,
  p_prev.close AS close_prev_day,
  p_now.close  AS close_event_day,
  p_next.close AS close_next_day
FROM events e
LEFT JOIN prices p_prev
  ON p_prev.symbol = e.symbol AND p_prev.date = date_sub(e.event_date, 1)
LEFT JOIN prices p_now
  ON p_now.symbol = e.symbol AND p_now.date = e.event_date
LEFT JOIN prices p_next
  ON p_next.symbol = e.symbol AND p_next.date = date_add(e.event_date, 1)
ORDER BY e.event_date, e.symbol
""")
display(validation)
