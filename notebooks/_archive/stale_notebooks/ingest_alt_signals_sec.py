# Databricks notebook source
# MAGIC %md
# MAGIC # 🧾 Alternative Signals (SEC)
# MAGIC
# MAGIC **Purpose**: Ingest SEC-based fundamentals and insider Form 4 signals.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `riskbricks.gold.sec_fundamentals`
# MAGIC - `riskbricks.gold.insider_form4`

# COMMAND ----------

dbutils.widgets.text("as_of_date", "", "As of date (YYYY-MM-DD)")
dbutils.widgets.text("max_symbols", "0", "Max symbols (0 = all)")
dbutils.widgets.text("sleep_seconds", "0.2", "Sleep between symbols")
dbutils.widgets.dropdown("mode", "both", ["both", "fundamentals", "form4"], "Mode")

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import time
import requests
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType

catalog = "riskbricks"
gold_db = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")

local_tz = ZoneInfo("America/New_York")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if not as_of_date:
    as_of_date = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")

max_symbols = int(dbutils.widgets.get("max_symbols") or "0")
sleep_seconds = float(dbutils.widgets.get("sleep_seconds") or "0.2")
mode = dbutils.widgets.get("mode").strip().lower()

run_fundamentals = mode in ("both", "fundamentals")
run_form4 = mode in ("both", "form4")

headers = {
    "User-Agent": "RiskBricks Research research@riskbricks.com",
    "Accept": "application/json",
}

def load_sec_ticker_map():
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    mapping = {}
    for _, entry in data.items():
        ticker = entry.get("ticker")
        cik = entry.get("cik_str")
        if ticker and cik:
            mapping[ticker.upper()] = str(cik).zfill(10)
    return mapping

print("✅ Loading SEC ticker map...")
sec_map = load_sec_ticker_map()

symbols_df = spark.sql("""
    SELECT DISTINCT symbol
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""")
symbols = [row.symbol for row in symbols_df.collect()]
if max_symbols and max_symbols > 0:
    symbols = symbols[:max_symbols]

print(f"✅ As of date: {as_of_date}")
print(f"✅ Symbols: {len(symbols)}")

# COMMAND ----------

def latest_fact(companyfacts, tag, unit_key):
    try:
        facts = companyfacts["facts"]["us-gaap"][tag]["units"][unit_key]
        facts = sorted(facts, key=lambda x: x.get("end") or "", reverse=True)
        return facts[0] if facts else None
    except Exception:
        return None

fundamentals_rows = []
form4_rows = []

def _to_float(val):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

for sym in symbols:
    cik = sec_map.get(sym)
    if not cik:
        continue

    try:
        if run_fundamentals:
            # Company facts for fundamentals
            facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            facts_resp = requests.get(facts_url, headers=headers, timeout=30)
            if facts_resp.status_code == 200:
                facts = facts_resp.json()
                revenue = latest_fact(facts, "Revenues", "USD")
                eps_basic = latest_fact(facts, "EarningsPerShareBasic", "USD/shares")
                eps_diluted = latest_fact(facts, "EarningsPerShareDiluted", "USD/shares")

                fundamentals_rows.append({
                    "symbol": sym,
                    "cik": cik,
                    "as_of_date": as_of_date,
                    "revenue": _to_float(revenue.get("val")) if revenue else None,
                    "revenue_end_date": revenue.get("end") if revenue else None,
                    "eps_basic": _to_float(eps_basic.get("val")) if eps_basic else None,
                    "eps_basic_end_date": eps_basic.get("end") if eps_basic else None,
                    "eps_diluted": _to_float(eps_diluted.get("val")) if eps_diluted else None,
                    "eps_diluted_end_date": eps_diluted.get("end") if eps_diluted else None,
                    "source_url": facts_url,
                })

        if run_form4:
            # Form 4 (insider trades) - best effort
            submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            sub_resp = requests.get(submissions_url, headers=headers, timeout=30)
            if sub_resp.status_code == 200:
                data = sub_resp.json()
                filings = data.get("filings", {}).get("recent", {})
                forms = filings.get("form", [])
                dates = filings.get("filingDate", [])
                accessions = filings.get("accessionNumber", [])
                primary_docs = filings.get("primaryDocument", [])

                for i, form in enumerate(forms[:50]):
                    if form not in ["4", "4/A"]:
                        continue
                    filing_date = dates[i] if i < len(dates) else None
                    accession = accessions[i].replace("-", "") if i < len(accessions) else ""
                    primary_doc = primary_docs[i] if i < len(primary_docs) else ""

                    cik_int = str(int(cik))
                    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession}"
                    index_url = f"{base}/index.json"
                    xml_url = ""
                    try:
                        idx_resp = requests.get(index_url, headers=headers, timeout=20)
                        if idx_resp.status_code == 200:
                            files = idx_resp.json().get("directory", {}).get("item", [])
                            xml_files = [f["name"] for f in files if f["name"].lower().endswith(".xml")]
                            if primary_doc.lower().endswith(".xml"):
                                xml_url = f"{base}/{primary_doc}"
                            elif xml_files:
                                xml_url = f"{base}/{xml_files[0]}"
                    except Exception:
                        xml_url = ""

                    issuer_name = None
                    owner_name = None
                    txn_date = None
                    txn_code = None
                    txn_shares = None
                    txn_price = None
                    if xml_url:
                        try:
                            xml_resp = requests.get(xml_url, headers=headers, timeout=20)
                            if xml_resp.status_code == 200:
                                root = ET.fromstring(xml_resp.text)
                                for el in root.iter():
                                    tag = el.tag.split("}")[-1]
                                    if tag == "issuerName" and not issuer_name:
                                        issuer_name = el.text
                                    if tag == "rptOwnerName" and not owner_name:
                                        owner_name = el.text
                                for txn in root.iter():
                                    if txn.tag.split("}")[-1] == "nonDerivativeTransaction":
                                        for child in txn.iter():
                                            ctag = child.tag.split("}")[-1]
                                            if ctag == "transactionDate":
                                                for g in child.iter():
                                                    if g.tag.split("}")[-1] == "value":
                                                        txn_date = g.text
                                            if ctag == "transactionCode":
                                                txn_code = child.text
                                            if ctag == "transactionShares":
                                                for g in child.iter():
                                                    if g.tag.split("}")[-1] == "value":
                                                        txn_shares = g.text
                                            if ctag == "transactionPricePerShare":
                                                for g in child.iter():
                                                    if g.tag.split("}")[-1] == "value":
                                                        txn_price = g.text
                                        break
                        except Exception:
                            pass

                    form4_rows.append({
                        "symbol": sym,
                        "cik": cik,
                        "filing_date": filing_date,
                        "accession": accession,
                        "form_type": form,
                        "issuer_name": issuer_name,
                        "owner_name": owner_name,
                        "transaction_date": txn_date,
                        "transaction_code": txn_code,
                        "shares": _to_float(txn_shares),
                        "price": _to_float(txn_price),
                        "document_url": xml_url or f"{base}/{primary_doc}",
                        "as_of_date": as_of_date,
                    })

        time.sleep(sleep_seconds)
    except Exception:
        continue

# COMMAND ----------

def write_table(table_name, rows, schema):
    if not rows:
        if spark.catalog.tableExists(table_name):
            print(f"⚠️ No rows for {table_name}")
            return
        empty_df = spark.createDataFrame([], schema=schema)
        empty_df = empty_df.withColumn("ingestion_timestamp", F.lit(None).cast("timestamp"))
        empty_df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(table_name)
        print(f"✅ Created empty table {table_name}")
        return
    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    if not spark.catalog.tableExists(table_name):
        df.write.mode("overwrite").partitionBy("as_of_date", "symbol").saveAsTable(table_name)
    else:
        df.write.mode("append").saveAsTable(table_name)
    print(f"✅ Saved {len(rows)} rows to {table_name}")

def log_table_count(table_name, as_of_date_value):
    if not spark.catalog.tableExists(table_name):
        print(f"⚠️ Missing table: {table_name}")
        return
    count = spark.sql(
        f"SELECT COUNT(*) AS c FROM {table_name} WHERE as_of_date = '{as_of_date_value}'"
    ).collect()[0]["c"]
    print(f"📊 {table_name} rows for as_of_date={as_of_date_value}: {count}")

fund_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("cik", StringType(), True),
    StructField("as_of_date", StringType(), False),
    StructField("revenue", DoubleType(), True),
    StructField("revenue_end_date", StringType(), True),
    StructField("eps_basic", DoubleType(), True),
    StructField("eps_basic_end_date", StringType(), True),
    StructField("eps_diluted", DoubleType(), True),
    StructField("eps_diluted_end_date", StringType(), True),
    StructField("source_url", StringType(), True),
])

form4_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("cik", StringType(), True),
    StructField("filing_date", StringType(), True),
    StructField("accession", StringType(), True),
    StructField("form_type", StringType(), True),
    StructField("issuer_name", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_code", StringType(), True),
    StructField("shares", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("document_url", StringType(), True),
    StructField("as_of_date", StringType(), False),
])

write_table(f"{gold_db}.sec_fundamentals", fundamentals_rows, fund_schema)
write_table(f"{gold_db}.insider_form4", form4_rows, form4_schema)

log_table_count(f"{gold_db}.sec_fundamentals", as_of_date)
log_table_count(f"{gold_db}.insider_form4", as_of_date)

dbutils.notebook.exit("✅ SEC alternative signals ingestion complete")
