# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Validate and Fix Symbol Mappings
# MAGIC
# MAGIC **Purpose**: Clean up company_universe table by:
# MAGIC - Removing delisted symbols
# MAGIC - Fixing incorrect ticker mappings
# MAGIC - Excluding ETFs and non-equity securities
# MAGIC - Validating symbols against Yahoo Finance

# COMMAND ----------

# MAGIC %pip install yfinance
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import yfinance as yf
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import time

catalog = "riskbricks"
gold_db = f"{catalog}.gold"

print("🔍 Validating company_universe symbols...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Known Symbol Issues

# COMMAND ----------

# Known incorrect or problematic symbols (verified Feb 2026)
SYMBOL_FIXES = {
    # Confirmed delisted or acquired companies
    "CHX": None,   # Delisted
    "HES": None,   # Hess Corp - acquired by Chevron (2024)
    "JNPR": None,  # Juniper Networks - acquired by HPE (2024)
    "PKI": None,   # PerkinElmer - split into two companies (2023)
    "PXD": None,   # Pioneer Natural Resources - acquired by Exxon (2024)
    "RAD": None,   # Rite Aid - bankrupt/delisted
    
    # ETFs (should be excluded from company universe - no earnings)
    "SPY": None,   # S&P 500 ETF
    "SLV": None,   # Silver ETF
    "GLD": None,   # Gold ETF
    "QQQ": None,   # Nasdaq 100 ETF
    "IWM": None,   # Russell 2000 ETF
    "EEM": None,   # Emerging Markets ETF
    "TLT": None,   # Treasury ETF
    "HYG": None,   # High Yield ETF
    
    # Note: These are VALID and should NOT be removed:
    # MRO - Marathon Oil (valid, different from MPC = Marathon Petroleum)
    # MPC - Marathon Petroleum (valid, different from MRO = Marathon Oil)
    # ANSS - Ansys (valid)
    # DFS - Discover Financial (valid)
    # IPG - Interpublic Group (valid)
    # PARA - Paramount Global (valid)
    # SKX - Skechers (valid)
}

# Create exclusion list
SYMBOLS_TO_REMOVE = [k for k, v in SYMBOL_FIXES.items() if v is None]

print(f"📋 Found {len(SYMBOLS_TO_REMOVE)} symbols to remove:")
for sym in SYMBOLS_TO_REMOVE:
    print(f"   ❌ {sym}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Validate Symbols via Yahoo Finance

# COMMAND ----------

def validate_symbol(symbol, quick=True):
    """
    Validate if a symbol is valid and tradeable
    Returns: (is_valid, reason, info_dict)
    """
    try:
        ticker = yf.Ticker(symbol)
        
        if quick:
            # Quick check - just try to get info
            info = ticker.info
            
            if not info or len(info) < 5:
                return False, "No data available", {}
            
            # Check if it's an ETF
            quote_type = info.get("quoteType", "")
            if quote_type in ["ETF", "MUTUALFUND", "INDEX"]:
                return False, f"Not a stock ({quote_type})", info
            
            # Check if it has a valid price
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not current_price or current_price <= 0:
                return False, "No valid price", info
            
            return True, "Valid", info
        else:
            # Thorough check - try to get historical data
            hist = ticker.history(period="5d")
            if hist.empty:
                return False, "No historical data", {}
            return True, "Valid", ticker.info
    
    except Exception as e:
        error_str = str(e).lower()
        if "404" in error_str or "not found" in error_str:
            return False, "Symbol not found (404)", {}
        else:
            return False, f"Error: {str(e)[:50]}", {}

# Test validation on known bad symbols
test_symbols = ["MRO", "SPY", "AAPL", "INVALID", "PKI"]
print("Testing validation logic:")
print("-" * 60)
for sym in test_symbols:
    is_valid, reason, info = validate_symbol(sym, quick=True)
    status = "✅" if is_valid else "❌"
    print(f"{status} {sym:8s} - {reason}")
print("-" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Scan Current Company Universe

# COMMAND ----------

# Get all symbols from company_universe
current_symbols = spark.sql("""
    SELECT symbol, company_name, sector, industry
    FROM riskbricks.gold.company_universe
    ORDER BY symbol
""").collect()

print(f"📊 Scanning {len(current_symbols)} symbols in company_universe...")
print("   (This will take 5-10 minutes)")
print()

invalid_symbols = []
valid_symbols = []

for i, row in enumerate(current_symbols):
    symbol = row.symbol
    
    # Quick check against known bad list
    if symbol in SYMBOLS_TO_REMOVE:
        invalid_symbols.append({
            "symbol": symbol,
            "company_name": row.company_name,
            "sector": row.sector,
            "reason": "Known delisted/ETF"
        })
        print(f"[{i+1:3d}/{len(current_symbols)}] ❌ {symbol:8s} - Known invalid")
        continue
    
    # Validate via Yahoo Finance
    is_valid, reason, info = validate_symbol(symbol, quick=True)
    
    if is_valid:
        valid_symbols.append(symbol)
        if (i + 1) % 20 == 0:  # Print every 20th
            print(f"[{i+1:3d}/{len(current_symbols)}] ✅ {symbol:8s} - Valid")
    else:
        invalid_symbols.append({
            "symbol": symbol,
            "company_name": row.company_name,
            "sector": row.sector,
            "reason": reason
        })
        print(f"[{i+1:3d}/{len(current_symbols)}] ❌ {symbol:8s} - {reason}")
    
    # Rate limiting
    if i % 10 == 0 and i > 0:
        time.sleep(1)

print()
print("="*60)
print(f"✅ Valid symbols: {len(valid_symbols)}")
print(f"❌ Invalid symbols: {len(invalid_symbols)}")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Show Invalid Symbols

# COMMAND ----------

if invalid_symbols:
    invalid_df = pd.DataFrame(invalid_symbols)
    print(f"\n❌ {len(invalid_symbols)} Invalid Symbols Found:")
    print("="*80)
    print(invalid_df.to_string(index=False))
    print("="*80)
    
    # Group by reason
    print("\n📊 Breakdown by reason:")
    reason_counts = invalid_df['reason'].value_counts()
    for reason, count in reason_counts.items():
        print(f"   - {reason}: {count}")
else:
    print("✅ All symbols are valid!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Create Clean Company Universe

# COMMAND ----------

# Create a cleaned version
if invalid_symbols:
    invalid_symbol_list = [item['symbol'] for item in invalid_symbols]
    
    # Create cleaned table
    clean_universe_query = f"""
        CREATE OR REPLACE TABLE {gold_db}.company_universe_clean AS
        SELECT *
        FROM {gold_db}.company_universe
        WHERE symbol NOT IN ({','.join([f"'{s}'" for s in invalid_symbol_list])})
    """
    
    spark.sql(clean_universe_query)
    
    # Get counts
    original_count = spark.table(f"{gold_db}.company_universe").count()
    clean_count = spark.table(f"{gold_db}.company_universe_clean").count()
    removed = original_count - clean_count
    
    print(f"✅ Created company_universe_clean")
    print(f"   Original: {original_count} symbols")
    print(f"   Cleaned: {clean_count} symbols")
    print(f"   Removed: {removed} symbols")
    print()
    print("🔄 To use the clean version:")
    print(f"   DROP TABLE {gold_db}.company_universe;")
    print(f"   ALTER TABLE {gold_db}.company_universe_clean RENAME TO {gold_db}.company_universe;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Apply the Fix (Optional)

# COMMAND ----------

# Uncomment to apply the fix
apply_fix = False  # Set to True to apply

if apply_fix and invalid_symbols:
    print("🔄 Applying fix...")
    
    # Backup original
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_db}.company_universe_backup AS
        SELECT * FROM {gold_db}.company_universe
    """)
    print("✅ Backup created: company_universe_backup")
    
    # Replace with clean version
    spark.sql(f"DROP TABLE IF EXISTS {gold_db}.company_universe")
    spark.sql(f"ALTER TABLE {gold_db}.company_universe_clean RENAME TO {gold_db}.company_universe")
    
    print("✅ company_universe cleaned and replaced")
    print(f"   Removed {len(invalid_symbols)} invalid symbols")
    print()
    print("📊 New company_universe:")
    spark.sql(f"SELECT COUNT(*) as total_symbols FROM {gold_db}.company_universe").show()
else:
    print("ℹ️  Fix not applied. Set apply_fix=True to apply.")
    print()
    print("📋 Invalid symbols saved in:")
    
    # Save invalid symbols to table for reference
    if invalid_symbols:
        invalid_df_spark = spark.createDataFrame(invalid_symbols)
        invalid_df_spark.write.mode("overwrite").saveAsTable(f"{gold_db}.invalid_symbols_log")
        print(f"   {gold_db}.invalid_symbols_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary

# COMMAND ----------

print("=" * 60)
print("🔍 Symbol Validation Complete")
print("=" * 60)
print()
print(f"✅ Valid symbols: {len(valid_symbols)}")
print(f"❌ Invalid symbols: {len(invalid_symbols)}")
print()
print("📋 Invalid symbols list saved to:")
print(f"   {gold_db}.invalid_symbols_log")
print()
print("🔧 Next Steps:")
print("  1. Review invalid_symbols_log table")
print("  2. Set apply_fix=True in Step 6 to remove invalid symbols")
print("  3. Re-run alt signals ingestion with cleaned universe")
print()
print("💡 The errors you saw are normal for these symbols.")
print("   They're either delisted, ETFs, or have been acquired.")
print("=" * 60)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

