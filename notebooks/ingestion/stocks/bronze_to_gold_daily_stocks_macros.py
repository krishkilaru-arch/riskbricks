# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion Pipeline - Free Public Data Sources
# MAGIC
# MAGIC This notebook ingests data from free public sources:
# MAGIC - **FRED**: Federal Reserve Economic Data (macro indicators)
# MAGIC - **Yahoo Finance**: Stock prices (via yfinance library - no API key needed!)
# MAGIC
# MAGIC **Data Range:** 2015-01-01 to Present (12 years of historical data)
# MAGIC
# MAGIC **Benefits:**
# MAGIC - ✅ No rate limits
# MAGIC - ✅ No API key required for Yahoo Finance
# MAGIC - ✅ 12 years of historical data covering multiple market cycles
# MAGIC - ✅ Works in any environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

from config import FRED_SERIES as _FRED_SERIES
from config.riskbricks_config import cfg as _cfg


# Install required libraries
%pip install yfinance requests pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import pyspark.sql.functions as F
from pyspark.sql.types import *

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# FRED API Key - Databricks secret
try:
    FRED_API_KEY = dbutils.secrets.get(scope=_cfg.SECRETS_SCOPE, key=_cfg.FRED_API_KEY_SECRET).strip()
    print("✅ Using FRED API key from secrets")
except Exception as exc:
    raise ValueError("Missing Databricks secret: scope=riskbricks key=fred-api-key") from exc

# Database/catalog setup
catalog = "riskbricks"
schema = "bronze"

# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schema: {schema}")

# Widgets (date range)
dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD)")
dbutils.widgets.text("macro_lookback_days", "365", "Macro lookback days (min history to fetch)")

from zoneinfo import ZoneInfo
local_tz = ZoneInfo("America/New_York")

start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
macro_lookback_days = int(dbutils.widgets.get("macro_lookback_days") or "365")

if not start_date or not end_date:
    yesterday = (datetime.now(local_tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = start_date or yesterday
    end_date = end_date or yesterday
    print(f"ℹ️ Defaulting to yesterday ET: {yesterday}")

def _to_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

today_et = datetime.now(local_tz).date()
start_dt = _to_date(start_date)
end_dt = _to_date(end_date)
if end_dt > today_et:
    end_dt = today_et
    end_date = end_dt.strftime("%Y-%m-%d")
    print(f"ℹ️ end_date capped to today ET: {end_date}")
if start_dt > end_dt:
    raise ValueError("start_date must be on or before end_date")

macro_start_dt = start_dt - timedelta(days=macro_lookback_days)
macro_start_date = macro_start_dt.strftime("%Y-%m-%d")
print(f"ℹ️ Macro lookback start: {macro_start_date} (days={macro_lookback_days})")

symbols_df = spark.sql(f"""
    SELECT DISTINCT symbol
    FROM {catalog}.gold.company_universe
    ORDER BY symbol
""")
selected_symbols = [row.symbol for row in symbols_df.collect()]
print(f"✅ Date range: {start_date} → {end_date}")
print(f"✅ Symbols loaded from company_universe: {len(selected_symbols)}")

def write_partitioned_table(table_name, df, partition_cols, date_col, start_dt, end_dt):
    """Write to Delta with partitions and safe date-range overwrite."""
    df = df.filter(
        (F.col(date_col) >= F.lit(start_dt).cast("date")) &
        (F.col(date_col) <= F.lit(end_dt).cast("date"))
    )
    if not spark.catalog.tableExists(table_name):
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    else:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
            if not detail.get("partitionColumns"):
                print(f"⚠️ Table {table_name} is not partitioned. Consider rebuilding with partitionBy{partition_cols}.")
        except Exception:
            pass
        replace_where = f"{date_col} >= '{start_dt}' AND {date_col} <= '{end_dt}'"
        df.write \
            .mode("overwrite") \
            .option("replaceWhere", replace_where) \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

def ensure_table_columns(table_name, columns):
    """
    Add missing columns to an existing Delta table (required when ACLs block auto-merge).
    columns: list of (name, dataTypeSql)
    """
    if not spark.catalog.tableExists(table_name):
        return
    existing = {row.col_name for row in spark.sql(f"DESCRIBE {table_name}").collect()}
    missing = [(name, dtype) for name, dtype in columns if name not in existing]
    if not missing:
        return
    cols_sql = ", ".join([f"{name} {dtype}" for name, dtype in missing])
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")
    print(f"✅ Added columns to {table_name}: {', '.join([m[0] for m in missing])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. FRED Macro Economic Data Ingestion

# COMMAND ----------

def get_fred_series_meta(series_id):
    base_url = "https://api.stlouisfed.org/fred/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    try:
        response = requests.get(base_url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            series = data.get("seriess", [{}])[0] if data.get("seriess") else {}
            return {
                "series_title": series.get("title"),
                "units": series.get("units"),
                "frequency": series.get("frequency"),
                "seasonal_adjustment": series.get("seasonal_adjustment"),
                "notes": series.get("notes"),
            }
        print(f"❌ FRED series metadata error for {series_id}: {response.status_code}")
    except Exception as e:
        print(f"❌ Error fetching series metadata {series_id}: {str(e)}")
    return {
        "series_title": None,
        "units": None,
        "frequency": None,
        "seasonal_adjustment": None,
        "notes": None,
    }

def get_fred_series(series_id, start_date="2015-01-01"):
    """
    Fetch economic data from FRED API
    
    Args:
        series_id: FRED series identifier (e.g., 'FEDFUNDS')
        start_date: Start date for data (YYYY-MM-DD) - Default: 2015-01-01 (12 years)
    
    Returns:
        List of tuples: [(date, value), ...]
    """
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            meta = get_fred_series_meta(series_id)
            observations = [
                (
                    obs.get("date"),
                    float(obs["value"]) if obs.get("value") not in (None, ".") else None,
                    series_id,
                    obs.get("realtime_start"),
                    obs.get("realtime_end"),
                    meta.get("series_title"),
                    meta.get("units"),
                    meta.get("frequency"),
                    meta.get("seasonal_adjustment"),
                    meta.get("notes"),
                )
                for obs in data["observations"]
                if obs.get("value") != "."  # Skip missing values
            ]
            print(f"✅ Fetched {len(observations)} observations for {series_id}")
            return observations
        else:
            print(f"❌ FRED API error for {series_id}: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error fetching {series_id}: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Key Macro Indicators

# COMMAND ----------

print("📊 Fetching macro economic indicators from FRED...")

# Define indicators to fetch
fred_indicators = {
    **_FRED_SERIES  # loaded from config
}

# Fetch all indicators
all_macro_data = []
for series_id, description in fred_indicators.items():
    print(f"  Fetching: {description} ({series_id})...")
    data = get_fred_series(series_id, start_date=macro_start_date)
    all_macro_data.extend(data)

print(f"\n✅ Total macro data points fetched: {len(all_macro_data)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Macro Data to Bronze Table

# COMMAND ----------

if all_macro_data:
    # Create DataFrame
    schema_macro = StructType([
        StructField("date", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("indicator_name", StringType(), False),
        StructField("realtime_start", StringType(), True),
        StructField("realtime_end", StringType(), True),
        StructField("series_title", StringType(), True),
        StructField("units", StringType(), True),
        StructField("frequency", StringType(), True),
        StructField("seasonal_adjustment", StringType(), True),
        StructField("notes", StringType(), True),
    ])
    
    macro_df = spark.createDataFrame(all_macro_data, schema=schema_macro)
    
    # Convert date string to date type
    macro_df = macro_df.withColumn("date", F.to_date(F.col("date"))) \
        .withColumn("realtime_start", F.to_date(F.col("realtime_start"))) \
        .withColumn("realtime_end", F.to_date(F.col("realtime_end")))
    
    # Add metadata
    macro_df = macro_df.withColumn("ingestion_timestamp", F.current_timestamp())
    
    # Write to bronze table (partitioned)
    table_name = f"{catalog}.{schema}.macro_indicators_bronze"
    ensure_table_columns(table_name, [
        ("realtime_start", "DATE"),
        ("realtime_end", "DATE"),
        ("series_title", "STRING"),
        ("units", "STRING"),
        ("frequency", "STRING"),
        ("seasonal_adjustment", "STRING"),
        ("notes", "STRING"),
    ])
    write_partitioned_table(
        table_name=table_name,
        df=macro_df,
        partition_cols=("date", "indicator_name"),
        date_col="date",
        start_dt=macro_start_date,
        end_dt=end_date
    )
    
    print(f"✅ Saved {macro_df.count()} records to {table_name}")
    
    # Show sample
    print("\n📊 Sample macro data:")
    macro_df.orderBy(F.col("date").desc()).show(10)
else:
    print("⚠️  No macro data to save")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Yahoo Finance Stock Data Ingestion
# MAGIC
# MAGIC **Advantages over Alpha Vantage:**
# MAGIC - ✅ No API key required
# MAGIC - ✅ No rate limits
# MAGIC - ✅ Faster bulk downloads
# MAGIC - ✅ More reliable

# COMMAND ----------

def get_stock_data_yfinance(tickers, start_date, end_date=None, batch_size=50):
    """
    Fetch stock data from Yahoo Finance using yfinance
    
    Args:
        tickers: List of stock symbols or single symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD), defaults to today
    
    Returns:
        pandas DataFrame with stock data
    """
    if end_date is None:
        end_date = datetime.now(local_tz).strftime("%Y-%m-%d")

    def _normalize_ticker(symbol):
        return symbol.replace(".", "-")

    start_dt_local = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt_local = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    end_date_exclusive = end_dt_local.strftime("%Y-%m-%d")

    yf_symbols = [_normalize_ticker(t) for t in tickers]
    symbol_map = {_normalize_ticker(t): t for t in tickers}
    
    try:
        print(f"📊 Downloading {tickers} from {start_date} to {end_date}...")
        
        # Download data (can handle multiple tickers at once!)
        # Note: auto_adjust=False ensures we get 'Adj Close' as a separate column
        all_batches = []
        failed_batches = []
        for i in range(0, len(yf_symbols), batch_size):
            batch = yf_symbols[i:i + batch_size]
            batch_data = yf.download(
                batch if len(batch) > 1 else batch[0],
                start=start_date,
                end=end_date_exclusive,
                progress=False,
                auto_adjust=False,  # Keep 'Adj Close' as separate column
                actions=True,       # Include dividends/splits/capital gains when available
                group_by='ticker' if len(batch) > 1 else None
            )
            if batch_data is None or batch_data.empty:
                failed_batches.extend(batch)
                continue
            all_batches.append(batch_data)

        if failed_batches:
            print(f"⚠️ No data returned for {len(failed_batches)} tickers")
        if not all_batches:
            print("⚠️  No data returned for all batches")
            return None, symbol_map

        data = pd.concat(all_batches, axis=1)
        
        if data.empty:
            print(f"⚠️  No data returned for {tickers}")
            return None
        
        print(f"✅ Downloaded {len(data)} records")
        print(f"   Columns in download: {list(data.columns.get_level_values(0).unique()) if hasattr(data.columns, 'levels') else list(data.columns)}")
        return data, symbol_map
        
    except Exception as e:
        print(f"❌ Error downloading {tickers}: {str(e)}")
        return None, {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Portfolio Stocks - Fortune 500 Companies

# COMMAND ----------

# Import Fortune 500 portfolio
import sys
sys.path.append('{cfg.REPO_ROOT}/data')

try:
    from fortune_500_portfolio import get_fortune_500_symbols, generate_portfolio_allocations
    portfolio_stocks = get_fortune_500_symbols()
    print(f"📊 Fortune 500 Portfolio: {len(portfolio_stocks)} companies")
except Exception as e:
    print(f"   Import failed: {e}")
    # Fallback to inline definition if import fails
    print("⚠️  Using inline Fortune 500 list (import failed)")
    # Top 100 most liquid Fortune 500 stocks for demo
    portfolio_stocks = [
        # Technology
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "CSCO",
        "ADBE", "CRM", "INTC", "AMD", "QCOM", "TXN", "IBM", "NOW", "INTU", "AMAT",
        # Financials  
        "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "CB", "AXP",
        "V", "MA", "PYPL", "SPGI", "MCO",
        # Healthcare
        "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY",
        "AMGN", "GILD", "CVS", "CI", "HCA",
        # Consumer
        "WMT", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX", "COST", "PG", "KO",
        "PEP", "TGT", "DG",
        # Communication
        "NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS", "CHTR",
        # Energy
        "XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX",
        # Industrials
        "BA", "HON", "UPS", "RTX", "LMT", "CAT", "DE", "GE", "MMM", "UNP",
        # Materials
        "LIN", "APD", "ECL", "SHW", "FCX", "NEM",
        # Real Estate
        "AMT", "PLD", "CCI", "EQIX", "SPG",
        # Utilities
        "NEE", "DUK", "SO", "D", "AEP"
    ]
    print(f"📊 Top 100 Fortune 500 stocks: {len(portfolio_stocks)} companies")

allowed_symbols = ["NVDA", "MSFT", "COST"]
portfolio_stocks = [s for s in portfolio_stocks if s in allowed_symbols]
print(f"\n✅ Limited to {len(portfolio_stocks)} stocks: {', '.join(portfolio_stocks)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Stock Prices

# COMMAND ----------

print("📊 Fetching stock data from Yahoo Finance (2015-2026, 12 years)...")

# Use full company_universe symbols (no filtering)
portfolio_stocks = selected_symbols or portfolio_stocks

print(f"\n✅ Will fetch data for {len(portfolio_stocks)} stocks: {', '.join(portfolio_stocks)}")

# Fetch data for all stocks at once (efficient!)
stock_data, symbol_map = get_stock_data_yfinance(
    tickers=portfolio_stocks,
    start_date=start_date,
    end_date=end_date
)

if stock_data is not None:
    print(f"✅ Fetched data for {len(portfolio_stocks)} stocks")
    print(f"   Date range: {stock_data.index.min()} to {stock_data.index.max()}")
    print(f"   Total records: {len(stock_data)}")
else:
    print("❌ Failed to fetch stock data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and Save Stock Data

# COMMAND ----------

if stock_data is not None and not stock_data.empty:
    print("📊 Transforming multi-index data to long format...")
    
    # Reshape data from wide to long format using pandas stack
    # This is the most robust and efficient way to handle yfinance multi-index DataFrames
    try:
        # 1. Detect column levels
        if isinstance(stock_data.columns, pd.MultiIndex):
            print(f"   Multi-index columns detected: {stock_data.columns.names}")
            
            # Determine which level is the Ticker (usually Level 1 or named 'Ticker')
            symbol_level = 1
            if 'Ticker' in stock_data.columns.names:
                symbol_level = 'Ticker'
            elif any(s in stock_data.columns.get_level_values(0) for s in portfolio_stocks[:10]):
                symbol_level = 0
            
            print(f"   Stacking data using level: {symbol_level}")
            
            # 2. Stack the DataFrame to move symbols to rows
            # This converts columns (Price, Ticker) -> rows (Date, Ticker) with columns (Price)
            long_df = stock_data.stack(level=symbol_level, future_stack=True).reset_index()
            
            # Remove multi-index column names if present
            if hasattr(long_df.columns, 'name'):
                long_df.columns.name = None
            
            # Debug: Print actual columns after stack
            print(f"   Columns after stack: {list(long_df.columns)[:10]}")
            
            # 3. Standardize column names
            # Map common variations to our schema names
            rename_map = {
                'level_1': 'symbol',
                'Ticker': 'symbol',
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Adj Close': 'adj_close',
                'Dividends': 'dividends',
                'Stock Splits': 'stock_splits',
                'Capital Gains': 'capital_gains'
            }
            long_df = long_df.rename(columns=rename_map)
            
            print(f"   Columns after rename: {list(long_df.columns)[:10]}")
            
            # Ensure 'symbol' column exists (it might be named differently depending on stack results)
            if 'symbol' not in long_df.columns:
                # Find the column that isn't one of the standard price columns
                known_cols = [
                    'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close',
                    'dividends', 'stock_splits', 'capital_gains'
                ]
                potential_symbol_cols = [c for col in long_df.columns if (c := str(col).lower()) not in known_cols]
                if potential_symbol_cols:
                    long_df = long_df.rename(columns={potential_symbol_cols[0]: 'symbol'})
            
            # 4. Handle missing columns
            if 'adj_close' not in long_df.columns and 'close' in long_df.columns:
                long_df['adj_close'] = long_df['close']
            if 'dividends' not in long_df.columns:
                long_df['dividends'] = 0.0
            if 'stock_splits' not in long_df.columns:
                long_df['stock_splits'] = 0.0
            if 'capital_gains' not in long_df.columns:
                long_df['capital_gains'] = 0.0
            
            # 5. Clean data types + map back to original symbols
            long_df['symbol'] = long_df['symbol'].astype(str)
            if symbol_map:
                long_df['symbol'] = long_df['symbol'].map(lambda s: symbol_map.get(s, s))
            long_df = long_df[long_df['close'].notna()]
            
            # Convert pandas Timestamp to Python date for Spark compatibility
            if 'date' in long_df.columns:
                long_df['date'] = pd.to_datetime(long_df['date']).dt.date
            
            # Convert volume to integer (from float) for Spark LongType compatibility
            if 'volume' in long_df.columns:
                long_df['volume'] = long_df['volume'].fillna(0).astype('int64')
            
            # 6. Keep only the columns we need (drop any extras like 'Price')
            required_cols = [
                'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close',
                'dividends', 'stock_splits', 'capital_gains'
            ]
            available_cols = [col for col in required_cols if col in long_df.columns]
            long_df = long_df[available_cols]
            
            print(f"   Final columns: {list(long_df.columns)}")
            print(f"   Sample adj_close values: {long_df['adj_close'].head(3).tolist() if 'adj_close' in long_df.columns else 'NOT FOUND'}")
            
            # Convert to list of dicts for Spark
            stock_records = long_df.to_dict('records')
            
        else:
            # Single stock logic
            print("   Single stock data detected...")
            # ... (keep existing single stock logic) ...
            symbol = portfolio_stocks[0]
            stock_records = []
            for _, row in stock_data.reset_index().iterrows():
                if pd.notna(row['Close']):
                    # Convert pandas Timestamp to Python date
                    date_val = pd.to_datetime(row['Date']).date() if hasattr(row['Date'], 'date') else row['Date']
                    # Convert volume to int, handling NaN and float values
                    volume_val = int(row['Volume']) if 'Volume' in row and pd.notna(row['Volume']) else 0
                    stock_records.append({
                        'symbol': symbol,
                        'date': date_val,
                        'open': float(row['Open']) if 'Open' in row else None,
                        'high': float(row['High']) if 'High' in row else None,
                        'low': float(row['Low']) if 'Low' in row else None,
                        'close': float(row['Close']),
                        'volume': volume_val,
                        'adj_close': float(row['Adj Close']) if 'Adj Close' in row else float(row['Close']),
                        'dividends': float(row['Dividends']) if 'Dividends' in row and pd.notna(row['Dividends']) else 0.0,
                        'stock_splits': float(row['Stock Splits']) if 'Stock Splits' in row and pd.notna(row['Stock Splits']) else 0.0,
                        'capital_gains': float(row['Capital Gains']) if 'Capital Gains' in row and pd.notna(row['Capital Gains']) else 0.0,
                    })

    except Exception as e:
        print(f"   ❌ Transformation failed: {str(e)}")
        print("   Falling back to debug info...")
        print(f"   Columns found: {stock_data.columns.tolist()[:10]}")
        stock_records = []
    
    print(f"✅ Transformed {len(stock_records):,} stock records")
    
    # Convert to Spark DataFrame
    stock_schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("dividends", DoubleType(), True),
        StructField("stock_splits", DoubleType(), True),
        StructField("capital_gains", DoubleType(), True),
    ])
    
    stock_df = spark.createDataFrame(stock_records, schema=stock_schema)
    
    # Add metadata
    stock_df = stock_df.withColumn("ingestion_timestamp", F.current_timestamp())
    stock_df = stock_df.withColumn("price", F.col("adj_close"))  # Use adjusted close as main price
    
    # Write to bronze table (partitioned)
    table_name = f"{catalog}.{schema}.stock_prices_bronze"
    ensure_table_columns(table_name, [
        ("dividends", "DOUBLE"),
        ("stock_splits", "DOUBLE"),
        ("capital_gains", "DOUBLE"),
    ])
    write_partitioned_table(
        table_name=table_name,
        df=stock_df,
        partition_cols=("date", "symbol"),
        date_col="date",
        start_dt=start_date,
        end_dt=end_date
    )
    
    print(f"✅ Saved {stock_df.count()} records to {table_name}")
    
    # Show sample
    print("\n📊 Sample stock data:")
    stock_df.orderBy(F.col("date").desc(), F.col("symbol")).show(20)
else:
    print("⚠️  No stock data to save")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Portfolio Holdings Table - Fortune 500 Allocations

# COMMAND ----------

print("📊 Creating Fortune 500 portfolio holdings table...")

# Generate portfolio allocations
try:
    from fortune_500_portfolio import generate_portfolio_allocations, FORTUNE_500_COMPANIES
    
    # Generate allocations (sector-weighted, realistic)
    total_portfolio_value = 100_000_000  # $100M portfolio (realistic for institutional)
    allocations = generate_portfolio_allocations(total_value=total_portfolio_value, method="sector_weighted")
    
    print(f"✅ Generated allocations for {len(allocations)} companies")
    print(f"💰 Total portfolio value: ${total_portfolio_value:,.0f}")
    
    # Convert to list of tuples for DataFrame
    portfolio_holdings = [
        (symbol, sector, weight, value_usd)
        for symbol, company, sector, industry, weight, value_usd in allocations
    ]
    
except Exception as e:
    print(f"⚠️  Fortune 500 import failed: {e}")
    print("   Using simplified allocation...")
    
    # Fallback: Create allocations from stock list
    # Equal weight allocation
    num_stocks = len(portfolio_stocks)
    weight_per_stock = 1.0 / num_stocks
    total_value = 100_000_000  # $100M
    value_per_stock = total_value * weight_per_stock
    
    # Map stocks to sectors (simplified)
    sector_map = {
        # Tech
        **{s: "Technology" for s in ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "CSCO", "ADBE", "CRM", "INTC", "AMD", "QCOM", "TXN", "IBM", "NOW", "INTU", "AMAT"]},
        # Finance
        **{s: "Financials" for s in ["JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "CB", "AXP", "V", "MA", "PYPL", "SPGI", "MCO"]},
        # Healthcare
        **{s: "Healthcare" for s in ["UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY", "AMGN", "GILD", "CVS", "CI", "HCA"]},
        # Consumer
        **{s: "Consumer Discretionary" for s in ["WMT", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX", "TGT", "DG"]},
        **{s: "Consumer Staples" for s in ["COST", "PG", "KO", "PEP"]},
        # Communication
        **{s: "Communication Services" for s in ["NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS", "CHTR"]},
        # Energy
        **{s: "Energy" for s in ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX"]},
        # Industrials
        **{s: "Industrials" for s in ["BA", "HON", "UPS", "RTX", "LMT", "CAT", "DE", "GE", "MMM", "UNP"]},
        # Materials
        **{s: "Materials" for s in ["LIN", "APD", "ECL", "SHW", "FCX", "NEM"]},
        # Real Estate
        **{s: "Real Estate" for s in ["AMT", "PLD", "CCI", "EQIX", "SPG"]},
        # Utilities
        **{s: "Utilities" for s in ["NEE", "DUK", "SO", "D", "AEP"]},
    }
    
    portfolio_holdings = [
        (symbol, sector_map.get(symbol, "Other"), weight_per_stock, value_per_stock)
        for symbol in portfolio_stocks
    ]
    
    print(f"✅ Created equal-weight allocations for {len(portfolio_holdings)} stocks")

# Create DataFrame
portfolio_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("sector", StringType(), False),
    StructField("weight", DoubleType(), False),
    StructField("value_usd", DoubleType(), False)
])

portfolio_df = spark.createDataFrame(portfolio_holdings, schema=portfolio_schema)

# Add metadata
portfolio_df = portfolio_df.withColumn("as_of_date", F.current_date())
portfolio_df = portfolio_df.withColumn("portfolio_id", F.lit("FORTUNE_500_PORTFOLIO"))

# Write to bronze (will be promoted to gold layer later)
table_name = f"{catalog}.{schema}.portfolio_holdings_bronze"
portfolio_df.write.mode("overwrite").saveAsTable(table_name)

print(f"\n✅ Saved {portfolio_df.count()} holdings to {table_name}")

# Show sector allocation summary
print("\n📊 Portfolio Sector Allocation:")
portfolio_df.groupBy("sector").agg(
    F.count("*").alias("num_stocks"),
    F.sum("weight").alias("total_weight"),
    F.sum("value_usd").alias("total_value")
).orderBy(F.desc("total_weight")).show(truncate=False)

# Show top 20 holdings
print("\n📊 Top 20 Holdings:")
portfolio_df.orderBy(F.desc("value_usd")).select("symbol", "sector", "weight", "value_usd").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("📊 DATA INGESTION SUMMARY")
print("="*60)

# Count records in each table
try:
    macro_count = spark.table(f"{catalog}.{schema}.macro_indicators_bronze").count()
    print(f"✅ Macro Indicators: {macro_count:,} records")
except:
    print(f"⚠️  Macro Indicators: Table not created")

try:
    stock_count = spark.table(f"{catalog}.{schema}.stock_prices_bronze").count()
    print(f"✅ Stock Prices: {stock_count:,} records")
except:
    print(f"⚠️  Stock Prices: Table not created")

try:
    portfolio_count = spark.table(f"{catalog}.{schema}.portfolio_holdings_bronze").count()
    print(f"✅ Portfolio Holdings: {portfolio_count:,} holdings")
except:
    print(f"⚠️  Portfolio Holdings: Table not created")

print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏅 Promote to Gold (Daily)

# COMMAND ----------

gold_schema = f"{catalog}.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

# gold.stock_prices_daily removed — silver.stock_prices is the canonical source
macro_gold = f"{gold_schema}.macro_indicators_daily"

try:
    ensure_table_columns(macro_gold, [
        ("realtime_start", "DATE"),
        ("realtime_end", "DATE"),
        ("series_title", "STRING"),
        ("units", "STRING"),
        ("frequency", "STRING"),
        ("seasonal_adjustment", "STRING"),
        ("notes", "STRING"),
    ])
    bronze_macro_df = spark.table(f"{catalog}.{schema}.macro_indicators_bronze")
    write_partitioned_table(
        table_name=macro_gold,
        df=bronze_macro_df,
        partition_cols=("date", "indicator_name"),
        date_col="date",
        start_dt=start_date,
        end_dt=end_date
    )
    print(f"✅ Gold macro_indicators_daily updated: {macro_gold}")
except Exception as exc:
    print(f"⚠️ Gold macro_indicators_daily update skipped: {exc}")

print("\n💡 Next steps:")
print("1. Run 02_data_validation.py to validate and clean data")
print("2. Run 03_risk_analytics.py to compute risk metrics")
print("3. Run 04_agent_workflow.py to test multi-agent system")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Quality

# COMMAND ----------

print("\n📊 Data Quality Checks:")
print("-" * 60)

# Check macro data
print("\n1. Macro Indicators Coverage:")
macro_df = spark.table(f"{catalog}.{schema}.macro_indicators_bronze")
macro_df.groupBy("indicator_name").agg(
    F.count("*").alias("records"),
    F.min("date").alias("start_date"),
    F.max("date").alias("end_date")
).orderBy("indicator_name").show(truncate=False)

# Check stock data
print("\n2. Stock Prices Coverage:")
stock_df = spark.table(f"{catalog}.{schema}.stock_prices_bronze")
stock_df.groupBy("symbol").agg(
    F.count("*").alias("records"),
    F.min("date").alias("start_date"),
    F.max("date").alias("end_date"),
    F.avg("close").alias("avg_price")
).orderBy("symbol").show(truncate=False)

# Check for missing values
print("\n3. Data Completeness:")
missing_prices = stock_df.filter(F.col("close").isNull()).count()
total_prices = stock_df.count()
completeness = (1 - missing_prices / total_prices) * 100

print(f"   Stock prices completeness: {completeness:.2f}%")
print(f"   Missing records: {missing_prices}/{total_prices}")

print("-" * 60)
print("✅ Data ingestion complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Data Freshness & Scheduling
# MAGIC
# MAGIC ### For Live Demo - Schedule This Notebook:
# MAGIC
# MAGIC 1. **Go to**: Workflows → Jobs → Create Job
# MAGIC 2. **Task**: Run this notebook (`01_data_ingestion.py`)
# MAGIC 3. **Schedule Options**:
# MAGIC    - **Pre-Demo**: Run once at 5:00 PM ET (after market close)
# MAGIC    - **During Demo**: Every 15-30 minutes (shows live updates)
# MAGIC    - **Daily Production**: Once daily at 5:30 PM ET
# MAGIC
# MAGIC ### Market Data Update Frequencies:
# MAGIC - **Stock Prices**: Updated ~15 min delayed (Yahoo Finance free tier)
# MAGIC - **Macro Indicators**: Daily (FRED)
# MAGIC - **Real-time**: Upgrade to paid API (Polygon.io, IEX Cloud)

# COMMAND ----------

# Track data freshness
print("\n" + "=" * 80)
print("📊 DATA FRESHNESS REPORT")
print("=" * 80)

# Latest stock price date
latest_stock = spark.sql(f"""
    SELECT MAX(date) as latest_date, 
           MAX(ingestion_timestamp) as last_updated
    FROM {catalog}.{schema}.stock_prices_bronze
""").collect()[0]

print(f"📈 Stock Prices:")
print(f"   Latest market date: {latest_stock['latest_date']}")
print(f"   Last ingestion: {latest_stock['last_updated']}")

# Latest macro data
latest_macro = spark.sql(f"""
    SELECT MAX(date) as latest_date
    FROM {catalog}.{schema}.macro_indicators_bronze
""").collect()[0]

print(f"\n📊 Macro Indicators:")
print(f"   Latest data date: {latest_macro['latest_date']}")

# Data age warning
from datetime import datetime, timedelta
current_time = datetime.now()
data_date = latest_stock['latest_date']

if isinstance(data_date, str):
    data_date = datetime.strptime(data_date, '%Y-%m-%d').date()

days_old = (current_time.date() - data_date).days

print(f"\n⏰ Data Age: {days_old} days old")
if days_old == 0:
    print("   ✅ Data is current (today)")
elif days_old == 1:
    print("   ✅ Data is recent (yesterday)")
elif days_old <= 3:
    print("   ⚠️  Data is {days_old} days old - consider refreshing")
else:
    print(f"   ❌ Data is {days_old} days old - REFRESH RECOMMENDED!")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")
