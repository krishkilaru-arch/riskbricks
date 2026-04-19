# Future Work: RAG Knowledge Base

**Status**: Parked — not currently in use
**Last reviewed**: June 2025

---

## Why It Was Parked

The RAG pipeline was the most over-built and under-utilized part of RiskBricks:

* **10M bronze → 2.2M gold → 294K vector-ready** documents — massive processing cost
* The `query_rag` UC function only does `LIKE '%keyword%'` on titles — basic SQL, not semantic search
* The vector search index (endpoint + embeddings) was set up but **never wired into the function**
* 64% of gold.rag_corpus is raw GDELT events (1.3M GKG + 880K events) — low retrieval quality
* Symbol distribution is heavily skewed (BAC alone = 1.4M of 2.2M rows)
* The refresh notebook **re-scraped RSS and SEC EDGAR** instead of reading from existing bronze/gold tables

---

## What Exists Today (tables still in catalog)

| Table | Schema | Rows | Description |
|-------|--------|------|-------------|
| `rag_corpus` | bronze | 10M | Raw document corpus (RSS + SEC + GDELT + wiki + stock context) |
| `rag_documents` | silver | 1.7K | Cleaned/deduplicated documents |
| `rag_corpus` | gold | 2.2M | Processed documents for retrieval |
| `rag_corpus_vs` | gold | 294K | Vector-search-ready (has embedding_text column) |
| `rag_corpus_index` | gold | — | Vector search index (may be inactive) |
| `rag_evidence_log` | gold | 7.4M | Retrieval records |
| `rag_news_timeline` | gold | 1.1K | News timeline entries |
| `rag_sector_insights` | gold | 12 | Sector summary documents |
| `rag_stock_coverage` | gold | 161 | Per-symbol coverage stats |
| `rag_retrieval_metrics_daily` | gold | 2.2K | Daily retrieval performance |
| `rag_filing_tracker` | gold | 71 | Tracked SEC filings |
| `rag_document_summary` | gold | 7 | Corpus summary stats |

### UC Function

```sql
-- Current implementation (keyword search only)
CREATE FUNCTION riskbricks.agent_tools.query_rag(search_query STRING, symbol STRING DEFAULT NULL)
RETURNS TABLE (title STRING, content STRING, source STRING, doc_type STRING, symbol STRING, sector STRING, published_date TIMESTAMP)
RETURN
    SELECT title, LEFT(content, 500) AS content, source, doc_type, symbol, sector, published_date
    FROM riskbricks.gold.rag_corpus
    WHERE (LOWER(title) LIKE CONCAT('%', LOWER(search_query), '%')
           OR LOWER(content) LIKE CONCAT('%', LOWER(search_query), '%'))
      AND (symbol = query_rag.symbol OR query_rag.symbol IS NULL)
    ORDER BY published_date DESC
    LIMIT 10
```

### Related Notebooks

| Notebook | Purpose |
|----------|---------|
| `notebooks/jobs/daily_rag_corpus_refresh` | Daily refresh (RSS + SEC + GDELT → bronze.rag_corpus) |
| `notebooks/03_gold/rag/create_vector_search_index` | Creates vector search index on gold.rag_corpus |
| `notebooks/03_gold/rag/create_gold_analytics` | Builds gold RAG analytics tables |
| `notebooks/02_silver/rag/clean_to_silver` | Bronze → silver cleaning |
| `notebooks/00_bronze/rag/ingest_incremental_rag_corpus_sources_rss_sec_wiki_stock_gdelt` | Original bulk ingestion |

### Agent Integration

The Retrieval Agent (1 of 7 sub-agents) calls `query_rag`. It is defined in `notebooks/agents/riskbricks_agent_v2.py`:
```python
RETRIEVAL_TOOLS = _get_tools(["query_rag"])
retrieval_agent = create_react_agent(llm, RETRIEVAL_TOOLS, prompt=RETRIEVAL_PROMPT)
```

---

## To Reactivate Properly (v2 Roadmap)

1. **Fix `query_rag` to use vector search** instead of LIKE keyword matching
   - Wire it to `riskbricks_vs_endpoint` / `riskbricks.gold.rag_corpus_index`
   - Use `similarity_search()` for semantic retrieval

2. **Stop re-fetching data** — read from existing tables:
   - RSS → read from `bronze.news_rss_all` (already ingested by ml_data_ingestion)
   - SEC → read from `gold.sec_fundamentals` (already ingested by sec_fundamentals_refresh)
   - GDELT → read from `bronze.historical_news_gdelt` (already ingested by gdelt_refresh)

3. **Curate the corpus** — remove raw GDELT events, keep only:
   - RSS article summaries (high quality, readable)
   - SEC filing abstracts (structured financial context)
   - Earnings surprise commentary
   - Stock context summaries

4. **Fix symbol skew** — BAC has 1.4M rows, most symbols have < 5K. Implement per-symbol caps.

5. **Test end-to-end** — verify the Retrieval Agent actually returns useful context before re-enabling daily refresh.
