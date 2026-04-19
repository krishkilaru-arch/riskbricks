# Future Work: Alternative Signals & SEC Fundamentals

**Status**: Parked — data was ingested but never consumed by any UC function, agent, or downstream table
**Last reviewed**: April 2025

---

## Why It Was Parked

All 6 tables had the same problem: notebooks fetched data from Yahoo Finance and SEC EDGAR, wrote to gold tables, but **nothing ever read from them**. No UC function, no agent sub-agent, no other notebook consumed the data.

---

## Tables (dropped from catalog)

### Alt Signals (from Yahoo Finance yfinance API)

| Table | Rows | Schema |
|-------|------|--------|
| `gold.analyst_recommendations` | 19,802 | symbol, event_date, firm, to_grade, from_grade, action, as_of_date |
| `gold.options_iv_skew_daily` | 5,206 | symbol, expiration, call_iv, put_iv, iv_skew, as_of_date |
| `gold.short_interest_snapshot` | 5,200 | symbol, short_ratio, short_percent_float, shares_short, as_of_date |
| `gold.insider_form4` | 117,960 | symbol, filing_date, insider_name, title, transaction_type, shares, value, as_of_date |

### SEC Fundamentals (from SEC EDGAR XBRL API + yfinance)

| Table | Rows | Schema |
|-------|------|--------|
| `gold.sec_fundamentals` | 1,241 | symbol, company_name, cik, metric, value, unit, period_end, filed_date, form_type, fiscal_year, fiscal_period, as_of_date |
| `gold.earnings_calendar` | 1,853 | symbol, event_date, eps_estimate, eps_actual, surprise_pct, as_of_date |

---

## Notebooks (still in repo)

| Notebook | What It Does |
|----------|-------------|
| `notebooks/jobs/daily_alt_signals_refresh` | Fetches analyst ratings, options IV, short interest, insider trades from yfinance |
| `notebooks/jobs/daily_sec_fundamentals_refresh` | Fetches SEC XBRL financials + earnings calendar |
| `notebooks/ingestion/alt_signals/ingest_alt_signals_yfinance.py` | Original alt signals ingestion |
| `notebooks/ingestion/alt_signals/ingest_alt_signals_sec.py` | Original SEC ingestion |

---

## To Reactivate

1. **Create UC functions** to serve this data to the agent:
   - `get_analyst_recommendations(symbol)` — recent upgrades/downgrades
   - `get_insider_activity(symbol)` — insider buys/sells
   - `get_options_sentiment(symbol)` — IV skew as fear gauge
   - `get_short_interest(symbol)` — short squeeze candidates
   - `get_fundamentals(symbol)` — revenue, EPS, debt levels
   - `get_earnings_calendar(symbol)` — upcoming earnings dates

2. **Wire into agent** — add a Fundamentals Agent sub-agent with these tools

3. **Feed into ML model** — add features like:
   - `days_to_earnings` (already in ml_training_features as hardcoded 30)
   - `analyst_consensus` (% buy vs sell)
   - `insider_net_buying` (buy - sell ratio)
   - `iv_skew` (options market fear)
   - `short_ratio` (crowding indicator)

4. **Re-run the notebooks** to recreate the tables, then verify end-to-end
