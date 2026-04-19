# RAG Knowledge Base — Future Work

> **Status**: Parked. Infrastructure exists but delivers no value in current form.
> **Decision**: Removed from active architecture. Revisit when vector search is properly wired.

---

## What Was Built

A retrieval-augmented generation (RAG) pipeline that ingests documents from multiple sources into a unified corpus for AI agent retrieval.

### Tables Created

| Table | Schema | Rows | Description |
|-------|--------|------|-------------|
| `rag_corpus` | bronze | 10M | Raw document corpus (RSS + SEC + GDELT + Wiki + stock context) |
| `rag_documents` | silver | 1.7K | Cleaned/deduplicated documents |
| `rag_corpus` | gold | 2.2M | Processed corpus for retrieval |
| `rag_corpus_vs` | gold | 294K | Vector-search-ready subset with embedding_text column |
| `rag_corpus_index` | gold | — | Vector search index (may not be active) |
| `rag_evidence_log` | gold | 7.4M | Retrieval event records |
| `rag_news_timeline` | gold | 1.1K | News timeline entries |
| `rag_sector_insights` | gold | 12 | Sector-level summaries |
| `rag_stock_coverage` | gold | 161 | Per-symbol document coverage stats |
| `rag_retrieval_metrics_daily` | gold | 2.2K | Daily retrieval quality metrics |
| `rag_filing_tracker` | gold | 71 | SEC filing tracking |
| `rag_document_summary` | gold | 7 | High-level corpus summaries |

### UC Function

```sql
-- riskbricks.agent_tools.query_rag (currently registered)
SELECT title, LEFT(content, 500) AS content, source, doc_type, symbol, sector, published_date
FROM riskbricks.gold.rag_corpus
WHERE (LOWER(title) LIKE CONCAT('%', LOWER(search_query), '%')
       OR LOWER(content) LIKE CONCAT('%', LOWER(search_query), '%'))
  AND (symbol = query_rag.symbol OR query_rag.symbol IS NULL)
ORDER BY published_date DESC
LIMIT 10
```

### Notebooks

| Notebook | What It Does |
|----------|-------------|
| `notebooks/jobs/daily_rag_corpus_refresh` | Builds corpus from RSS + SEC + GDELT (re-fetches APIs) |
| `notebooks/03_gold/rag/create_vector_search_index` | Creates VS endpoint + index on rag_corpus |
| `notebooks/03_gold/rag/create_gold_analytics` | Builds evidence_log, timeline, sector insights |
| `notebooks/02_silver/rag/clean_to_silver` | Bronze → silver document cleaning |

### Agent Integration

The **Retrieval Agent** (1 of 7 sub-agents in `riskbricks_agent_v2.py`) uses `query_rag` as its only tool.

---

## Why It Was Parked

### Audit Findings (June 2025)

1. **query_rag is just a LIKE search** — not vector/semantic search. It does `WHERE LOWER(title) LIKE '%keyword%'`. You could run this directly on `bronze.news_rss_all` or `bronze.historical_news_gdelt` without building a 10M-row corpus.

2. **Vector search was set up but never connected** — `rag_corpus_vs` (294K rows) was prepared for embeddings, and `create_vector_search_index` notebook exists, but `query_rag` UC function still uses LIKE instead of `similarity_search()`.

3. **Corpus quality is poor** — 64% of gold.rag_corpus is raw GDELT events (1.3M GKG + 880K events). Symbol distribution is heavily skewed (BAC has 1.4M of 2.2M rows). Not useful for retrieval.

4. **Duplicates work from other notebooks**:
   - Re-scrapes RSS feeds (already done by `ml_data_ingestion`)
   - Re-calls SEC EDGAR API (already done by `daily_sec_fundamentals_refresh`)

5. **Massive pipeline for zero retrieval value** — 10M bronze → 2.2M gold → 294K vector-ready docs, plus 7 downstream gold tables, all feeding a LIKE search that returns 10 rows.

---

## What Would Make It Viable (v2 Roadmap)

### Step 1: Fix the UC function
Replace keyword LIKE with actual vector search:
```python
# In query_rag, replace SQL LIKE with:
vsc.get_index("riskbricks_vs_endpoint", "riskbricks.gold.rag_corpus_index").similarity_search(
    query_text=search_query,
    columns=["symbol", "title", "content", "source", "published_date"],
    num_results=10,
    filters={"symbol": symbol}  # optional filter
)
```

### Step 2: Curate the corpus
- Remove raw GDELT events (use daily summaries instead, ~38K rows not 2.2M)
- Add quality RSS articles only (not every feed item)
- Include SEC filing narratives (MD&A sections), not just XBRL numbers
- Target: ~50K high-quality documents, not 10M low-quality ones

### Step 3: Stop re-fetching
- Read RSS from `bronze.news_rss_all` instead of calling feedparser again
- Read SEC metadata from `gold.sec_fundamentals` instead of calling EDGAR again
- Read GDELT summaries from `pipelines.news_sentiment_daily` instead of re-aggregating

### Step 4: Rebuild the index
- Use `databricks-gte-large-en` embedding model (already configured in notebook)
- Delta Sync index on curated gold table
- Test semantic queries: "What happened with Apple earnings?" should return relevant docs

---

## Cleanup (when ready to delete)

To fully remove RAG infrastructure:
```sql
-- Drop tables (12 tables)
DROP TABLE IF EXISTS riskbricks.bronze.rag_corpus;
DROP TABLE IF EXISTS riskbricks.silver.rag_documents;
DROP TABLE IF EXISTS riskbricks.gold.rag_corpus;
DROP TABLE IF EXISTS riskbricks.gold.rag_corpus_vs;
DROP TABLE IF EXISTS riskbricks.gold.rag_evidence_log;
DROP TABLE IF EXISTS riskbricks.gold.rag_news_timeline;
DROP TABLE IF EXISTS riskbricks.gold.rag_sector_insights;
DROP TABLE IF EXISTS riskbricks.gold.rag_stock_coverage;
DROP TABLE IF EXISTS riskbricks.gold.rag_retrieval_metrics_daily;
DROP TABLE IF EXISTS riskbricks.gold.rag_filing_tracker;
DROP TABLE IF EXISTS riskbricks.gold.rag_document_summary;

-- Drop UC function
DROP FUNCTION IF EXISTS riskbricks.agent_tools.query_rag;

-- Delete vector search index (via Python)
-- vsc.delete_index("riskbricks_vs_endpoint", "riskbricks.gold.rag_corpus_index")
```

To remove the Retrieval Agent from the supervisor, edit `riskbricks_agent_v2.py`:
- Remove `RETRIEVAL_TOOLS`, `RETRIEVAL_PROMPT`, `retrieval_agent` from agent registry
- Remove "retrieval_agent" from `AGENT_NAMES` and supervisor routing
- Re-deploy agent (run `02_create_agent` → `03_deploy_agent`)
