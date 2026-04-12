# Databricks notebook source
# MAGIC %md
# MAGIC # RiskBricks: Multi-Manager Portfolio Setup
# MAGIC
# MAGIC Creates the complete data model for RiskBricks with three portfolio managers:
# MAGIC - **Sarah Russel**: Conservative Growth Strategy
# MAGIC - **Rena Tang**: Balanced Value Strategy  
# MAGIC - **Mohit Arora**: Aggressive Growth Strategy
# MAGIC
# MAGIC ## Tables Created:
# MAGIC 1. `company_universe` - Master list of all Fortune 500 companies
# MAGIC 2. `portfolio_managers` - Manager profiles and risk parameters
# MAGIC 3. `portfolios` - Portfolio metadata and performance
# MAGIC 4. `portfolio_holdings` - Actual stock positions per portfolio

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

# Databricks configuration
catalog = "riskbricks"
schema = "gold"  # Multi-manager portfolios go directly to gold layer

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Company Universe Table
# MAGIC
# MAGIC Master list of all Fortune 500 companies with risk metrics

# COMMAND ----------

print("📊 Creating Company Universe...")

# Base Fortune 500 list (symbol, company_name, sector, industry)
try:
    from data.fortune_500_portfolio import FORTUNE_500_COMPANIES
except Exception:
    import importlib.util
    import sys

    # Auto-detect repo root
import os as _os
_nb_ctx = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_nb_ctx))))
    module_path = f"{repo_root}/data/fortune_500_portfolio.py"
    spec = importlib.util.spec_from_file_location("fortune_500_portfolio", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["fortune_500_portfolio"] = module
    spec.loader.exec_module(module)
    FORTUNE_500_COMPANIES = module.FORTUNE_500_COMPANIES

# Add default risk fields for the Fortune 500 list
companies_data = [
    # Technology (High Growth)
    ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics", 1.2, 18.5, True, True),
    ("MSFT", "Microsoft Corporation", "Technology", "Software", 1.0, 16.2, True, True),
    ("GOOGL", "Alphabet Inc.", "Technology", "Internet", 1.1, 19.8, True, True),
    ("AMZN", "Amazon.com Inc.", "Technology", "Internet Retail", 1.3, 22.4, True, True),
    ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors", 1.7, 32.1, True, True),
    ("META", "Meta Platforms Inc.", "Technology", "Social Media", 1.4, 28.3, True, True),
    ("TSLA", "Tesla Inc.", "Consumer Discretionary", "Automobiles", 1.8, 35.7, True, True),
    ("AVGO", "Broadcom Inc.", "Technology", "Semiconductors", 1.2, 21.5, True, True),
    ("ORCL", "Oracle Corporation", "Technology", "Software", 1.0, 17.8, True, True),
    ("CSCO", "Cisco Systems Inc.", "Technology", "Networking", 0.9, 16.1, True, True),
    ("ADBE", "Adobe Inc.", "Technology", "Software", 1.2, 20.3, True, True),
    ("CRM", "Salesforce Inc.", "Technology", "Software", 1.3, 24.7, True, True),
    ("INTC", "Intel Corporation", "Technology", "Semiconductors", 1.1, 22.9, True, True),
    ("AMD", "Advanced Micro Devices", "Technology", "Semiconductors", 1.6, 31.2, True, True),
    ("QCOM", "Qualcomm Inc.", "Technology", "Semiconductors", 1.1, 19.7, True, True),
    ("TXN", "Texas Instruments", "Technology", "Semiconductors", 0.9, 18.4, True, True),
    ("IBM", "IBM Corporation", "Technology", "IT Services", 0.8, 15.6, True, True),
    ("NOW", "ServiceNow Inc.", "Technology", "Software", 1.4, 26.8, True, True),
    ("INTU", "Intuit Inc.", "Technology", "Software", 1.1, 19.5, True, True),
    ("AMAT", "Applied Materials", "Technology", "Semiconductors", 1.3, 23.1, True, True),
    
    # Financials (Medium Risk)
    ("JPM", "JPMorgan Chase & Co.", "Financials", "Banks", 1.0, 17.2, True, True),
    ("BAC", "Bank of America Corp", "Financials", "Banks", 1.1, 19.4, True, True),
    ("WFC", "Wells Fargo & Company", "Financials", "Banks", 1.0, 18.7, True, True),
    ("C", "Citigroup Inc.", "Financials", "Banks", 1.2, 21.3, True, True),
    ("GS", "Goldman Sachs Group", "Financials", "Investment Banking", 1.1, 20.5, True, True),
    ("MS", "Morgan Stanley", "Financials", "Investment Banking", 1.1, 19.8, True, True),
    ("BLK", "BlackRock Inc.", "Financials", "Asset Management", 1.0, 17.6, True, True),
    ("SCHW", "Charles Schwab Corp", "Financials", "Brokerage", 1.1, 18.9, True, True),
    ("CB", "Chubb Limited", "Financials", "Insurance", 0.9, 15.3, True, True),
    ("AXP", "American Express Co", "Financials", "Credit Services", 1.1, 19.1, True, True),
    ("V", "Visa Inc.", "Financials", "Payment Processing", 0.9, 16.7, True, True),
    ("MA", "Mastercard Inc.", "Financials", "Payment Processing", 0.9, 16.4, True, True),
    ("PYPL", "PayPal Holdings Inc.", "Financials", "Payment Processing", 1.3, 25.6, True, True),
    ("SPGI", "S&P Global Inc.", "Financials", "Financial Data", 0.9, 17.1, True, True),
    ("MCO", "Moody's Corporation", "Financials", "Financial Data", 0.9, 17.8, True, True),
    
    # Healthcare (Low-Medium Risk)
    ("UNH", "UnitedHealth Group", "Healthcare", "Health Insurance", 0.8, 14.2, True, True),
    ("JNJ", "Johnson & Johnson", "Healthcare", "Pharmaceuticals", 0.7, 12.1, True, True),
    ("LLY", "Eli Lilly and Company", "Healthcare", "Pharmaceuticals", 0.8, 15.7, True, True),
    ("PFE", "Pfizer Inc.", "Healthcare", "Pharmaceuticals", 0.7, 13.4, True, True),
    ("ABBV", "AbbVie Inc.", "Healthcare", "Pharmaceuticals", 0.8, 14.8, True, True),
    ("MRK", "Merck & Co. Inc.", "Healthcare", "Pharmaceuticals", 0.7, 13.2, True, True),
    ("TMO", "Thermo Fisher Scientific", "Healthcare", "Life Sciences", 0.8, 15.1, True, True),
    ("ABT", "Abbott Laboratories", "Healthcare", "Medical Devices", 0.8, 14.3, True, True),
    ("DHR", "Danaher Corporation", "Healthcare", "Life Sciences", 0.9, 16.2, True, True),
    ("BMY", "Bristol-Myers Squibb", "Healthcare", "Pharmaceuticals", 0.8, 14.9, True, True),
    ("AMGN", "Amgen Inc.", "Healthcare", "Biotechnology", 0.8, 15.6, True, True),
    ("GILD", "Gilead Sciences Inc.", "Healthcare", "Biotechnology", 0.9, 17.2, True, True),
    ("CVS", "CVS Health Corporation", "Healthcare", "Healthcare Services", 0.9, 16.8, True, True),
    ("CI", "Cigna Corporation", "Healthcare", "Health Insurance", 0.8, 15.4, True, True),
    ("HCA", "HCA Healthcare Inc.", "Healthcare", "Healthcare Facilities", 1.0, 18.3, True, True),
    
    # Consumer Staples (Low Risk)
    ("WMT", "Walmart Inc.", "Consumer Staples", "Discount Stores", 0.7, 12.3, True, True),
    ("PG", "Procter & Gamble Co", "Consumer Staples", "Household Products", 0.6, 11.2, True, True),
    ("KO", "Coca-Cola Company", "Consumer Staples", "Beverages", 0.6, 11.8, True, True),
    ("PEP", "PepsiCo Inc.", "Consumer Staples", "Beverages", 0.6, 11.5, True, True),
    ("COST", "Costco Wholesale Corp", "Consumer Staples", "Warehouse Clubs", 0.8, 14.6, True, True),
    
    # Consumer Discretionary (Medium-High Risk)
    ("HD", "Home Depot Inc.", "Consumer Discretionary", "Home Improvement", 1.0, 17.9, True, True),
    ("MCD", "McDonald's Corporation", "Consumer Discretionary", "Restaurants", 0.8, 14.7, True, True),
    ("NKE", "Nike Inc.", "Consumer Discretionary", "Apparel", 1.0, 18.6, True, True),
    ("SBUX", "Starbucks Corporation", "Consumer Discretionary", "Restaurants", 0.9, 16.9, True, True),
    ("LOW", "Lowe's Companies Inc.", "Consumer Discretionary", "Home Improvement", 1.0, 18.2, True, True),
    ("TJX", "TJX Companies Inc.", "Consumer Discretionary", "Apparel Retail", 0.9, 16.1, True, True),
    ("TGT", "Target Corporation", "Consumer Discretionary", "General Merchandise", 1.0, 19.3, True, True),
    ("DG", "Dollar General Corp", "Consumer Discretionary", "Discount Stores", 0.9, 17.4, True, True),
    
    # Communication Services (Medium-High Risk)
    ("NFLX", "Netflix Inc.", "Communication Services", "Entertainment", 1.5, 29.4, True, True),
    ("DIS", "Walt Disney Company", "Communication Services", "Entertainment", 1.1, 20.7, True, True),
    ("CMCSA", "Comcast Corporation", "Communication Services", "Cable", 0.9, 17.3, True, True),
    ("T", "AT&T Inc.", "Communication Services", "Telecom", 0.7, 14.1, True, True),
    ("VZ", "Verizon Communications", "Communication Services", "Telecom", 0.7, 13.8, True, True),
    ("TMUS", "T-Mobile US Inc.", "Communication Services", "Wireless Telecom", 1.0, 18.9, True, True),
    ("CHTR", "Charter Communications", "Communication Services", "Cable", 1.1, 21.4, True, True),
    
    # Energy (Medium Risk)
    ("XOM", "Exxon Mobil Corporation", "Energy", "Oil & Gas", 1.0, 19.6, True, True),
    ("CVX", "Chevron Corporation", "Energy", "Oil & Gas", 1.0, 19.2, True, True),
    ("COP", "ConocoPhillips", "Energy", "Oil & Gas", 1.2, 23.7, True, True),
    ("SLB", "Schlumberger Limited", "Energy", "Oilfield Services", 1.3, 26.1, True, True),
    ("EOG", "EOG Resources Inc.", "Energy", "Oil & Gas", 1.2, 24.3, True, True),
    ("MPC", "Marathon Petroleum Corp", "Energy", "Oil & Gas Refining", 1.1, 21.8, True, True),
    ("PSX", "Phillips 66", "Energy", "Oil & Gas Refining", 1.1, 22.4, True, True),
    
    # Industrials (Medium Risk)
    ("BA", "Boeing Company", "Industrials", "Aerospace", 1.3, 25.9, True, True),
    ("HON", "Honeywell International", "Industrials", "Conglomerate", 0.9, 16.7, True, True),
    ("UPS", "United Parcel Service", "Industrials", "Package Delivery", 0.9, 17.1, True, True),
    ("RTX", "Raytheon Technologies", "Industrials", "Aerospace & Defense", 0.9, 17.4, True, True),
    ("LMT", "Lockheed Martin Corp", "Industrials", "Aerospace & Defense", 0.8, 15.8, True, True),
    ("CAT", "Caterpillar Inc.", "Industrials", "Construction Equipment", 1.1, 20.6, True, True),
    ("DE", "Deere & Company", "Industrials", "Agricultural Equipment", 1.0, 19.7, True, True),
    ("GE", "General Electric Co", "Industrials", "Conglomerate", 1.2, 22.8, True, True),
    ("MMM", "3M Company", "Industrials", "Conglomerate", 0.9, 17.2, True, True),
    ("UNP", "Union Pacific Corporation", "Industrials", "Railroads", 0.9, 17.6, True, True),
    
    # Materials (Medium Risk)
    ("LIN", "Linde plc", "Materials", "Chemicals", 0.9, 16.3, True, True),
    ("APD", "Air Products & Chemicals", "Materials", "Chemicals", 0.8, 15.7, True, True),
    ("ECL", "Ecolab Inc.", "Materials", "Chemicals", 0.8, 15.9, True, True),
    ("SHW", "Sherwin-Williams Company", "Materials", "Chemicals", 0.9, 17.4, True, True),
    ("FCX", "Freeport-McMoRan Inc.", "Materials", "Copper", 1.4, 27.6, True, True),
    ("NEM", "Newmont Corporation", "Materials", "Gold", 1.2, 24.1, True, True),
    
    # Real Estate (Low-Medium Risk)
    ("AMT", "American Tower Corp", "Real Estate", "REITs", 0.8, 16.2, True, True),
    ("PLD", "Prologis Inc.", "Real Estate", "REITs", 0.9, 17.1, True, True),
    ("CCI", "Crown Castle Inc.", "Real Estate", "REITs", 0.8, 16.7, True, True),
    ("EQIX", "Equinix Inc.", "Real Estate", "REITs", 0.9, 17.9, True, True),
    ("SPG", "Simon Property Group", "Real Estate", "REITs", 1.1, 21.3, True, True),
    
    # Utilities (Low Risk)
    ("NEE", "NextEra Energy Inc.", "Utilities", "Electric Utilities", 0.7, 13.2, True, True),
    ("DUK", "Duke Energy Corporation", "Utilities", "Electric Utilities", 0.6, 11.8, True, True),
    ("SO", "Southern Company", "Utilities", "Electric Utilities", 0.6, 11.4, True, True),
    ("D", "Dominion Energy Inc.", "Utilities", "Electric Utilities", 0.7, 12.6, True, True),
    ("AEP", "American Electric Power", "Utilities", "Electric Utilities", 0.6, 11.9, True, True),
]

# Override with full Fortune 500 list
DEFAULT_BETA = 1.0
DEFAULT_VOL_30D = 20.0
companies_data = [
    (symbol, name, sector, industry, DEFAULT_BETA, DEFAULT_VOL_30D, False, True)
    for symbol, name, sector, industry in FORTUNE_500_COMPANIES
]

# Add common ETFs/indices that aren't in Fortune 500
extra_companies = [
    ("SPY", "SPDR S&P 500 ETF Trust", "ETF", "Index", 1.0, 15.0, False, False),
    ("SLV", "iShares Silver Trust", "ETF", "Commodities", 1.0, 20.0, False, False),
]

# Create DataFrame
company_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("company_name", StringType(), False),
    StructField("sector", StringType(), False),
    StructField("industry", StringType(), False),
    StructField("beta", DoubleType(), False),
    StructField("volatility_30d", DoubleType(), False),
    StructField("is_sp500", BooleanType(), False),
    StructField("is_fortune500", BooleanType(), False)
])

# Add calculated/updated fields after initial creation
companies_df = spark.createDataFrame(companies_data, schema=company_schema)
if extra_companies:
    extra_df = spark.createDataFrame(extra_companies, schema=company_schema)
    companies_df = companies_df.unionByName(extra_df)

# Add new columns with default values (will be updated by data ingestion)
companies_df = companies_df.withColumn("volatility_90d", F.lit(None).cast(DoubleType())) \
    .withColumn("avg_volume_30d", F.lit(None).cast(LongType())) \
    .withColumn("market_cap_usd", F.lit(None).cast(DoubleType())) \
    .withColumn("latest_price", F.lit(None).cast(DoubleType())) \
    .withColumn("price_change_1d_pct", F.lit(None).cast(DoubleType())) \
    .withColumn("price_change_1w_pct", F.lit(None).cast(DoubleType())) \
    .withColumn("price_change_1m_pct", F.lit(None).cast(DoubleType())) \
    .withColumn("dividend_yield", F.lit(None).cast(DoubleType())) \
    .withColumn("pe_ratio", F.lit(None).cast(DoubleType())) \
    .withColumn("country", F.lit("USA")) \
    .withColumn("currency", F.lit("USD")) \
    .withColumn("computed_at", F.current_timestamp()) \
    .withColumn("price_updated_at", F.lit(None).cast(TimestampType()))

# Write hardcoded companies first
table_name = f"{catalog}.{schema}.company_universe"
companies_df.write.mode("overwrite").saveAsTable(table_name)

print(f"✅ Created base company_universe with {companies_df.count()} Fortune 500 companies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Expand Company Universe with ALL Stocks from Stock Prices
# MAGIC
# MAGIC Add any additional stocks that exist in stock_prices but aren't in our hardcoded list.
# MAGIC This ensures the company_universe includes ALL tradeable securities for RAG and analytics.

# COMMAND ----------

print("📊 Expanding Company Universe with all stocks from stock_prices...")

# Check if stock_prices table exists
try:
    # Get all unique symbols from stock prices (silver or bronze)
    try:
        all_stock_symbols = spark.sql("""
            SELECT DISTINCT symbol FROM riskbricks.silver.stock_prices
        """)
        source_table = "silver.stock_prices"
    except:
        all_stock_symbols = spark.sql("""
            SELECT DISTINCT symbol FROM riskbricks.bronze.stock_prices_bronze
        """)
        source_table = "bronze.stock_prices_bronze"
    
    total_stock_symbols = all_stock_symbols.count()
    print(f"   Found {total_stock_symbols} unique symbols in {source_table}")
    
    # Get existing symbols in company_universe
    existing_symbols = spark.sql(f"""
        SELECT DISTINCT symbol FROM {catalog}.{schema}.company_universe
    """)
    existing_count = existing_symbols.count()
    print(f"   Existing symbols in company_universe: {existing_count}")
    
    # Find symbols that are in stock_prices but NOT in company_universe
    missing_symbols = all_stock_symbols.subtract(existing_symbols)
    if allowed_symbols:
        missing_symbols = missing_symbols.filter(F.col("symbol").isin(allowed_symbols))
    missing_count = missing_symbols.count()
    print(f"   Missing symbols to add: {missing_count}")
    
    if missing_count > 0:
        # Create records for missing symbols with default values
        # We'll infer sector from the symbol or use 'Unknown'
        missing_companies = missing_symbols.select(
            F.col("symbol"),
            F.concat(F.col("symbol"), F.lit(" Inc.")).alias("company_name"),
            F.lit("Unknown").alias("sector"),
            F.lit("Unknown").alias("industry"),
            F.lit(1.0).alias("beta"),  # Default beta
            F.lit(20.0).alias("volatility_30d"),  # Default volatility
            F.lit(False).alias("is_sp500"),
            F.lit(False).alias("is_fortune500"),
            F.lit(None).cast(DoubleType()).alias("volatility_90d"),
            F.lit(None).cast(LongType()).alias("avg_volume_30d"),
            F.lit(None).cast(DoubleType()).alias("market_cap_usd"),
            F.lit(None).cast(DoubleType()).alias("latest_price"),
            F.lit(None).cast(DoubleType()).alias("price_change_1d_pct"),
            F.lit(None).cast(DoubleType()).alias("price_change_1w_pct"),
            F.lit(None).cast(DoubleType()).alias("price_change_1m_pct"),
            F.lit(None).cast(DoubleType()).alias("dividend_yield"),
            F.lit(None).cast(DoubleType()).alias("pe_ratio"),
            F.lit("USA").alias("country"),
            F.lit("USD").alias("currency"),
            F.current_timestamp().alias("computed_at"),
            F.lit(None).cast(TimestampType()).alias("price_updated_at")
        )
        
        # Append to company_universe
        missing_companies.write.mode("append").saveAsTable(table_name)
        print(f"   ✅ Added {missing_count} additional symbols to company_universe")
    
    # Show final count
    final_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0].cnt
    print(f"\n✅ Final company_universe count: {final_count} stocks")
    
except Exception as e:
    print(f"⚠️ Could not expand company_universe: {e}")
    print("   Stock prices may not be ingested yet. Run 02_data_ingestion.py first.")

# COMMAND ----------

# Show summary
print("\n📊 Companies by Sector:")
spark.sql(f"""
    SELECT sector, COUNT(*) as count 
    FROM {catalog}.{schema}.company_universe 
    GROUP BY sector 
    ORDER BY count DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Portfolio Managers Table

# COMMAND ----------

print("👥 Creating Portfolio Managers...")

managers_data = [
    (
        "PM001",
        "Sarah Russel",
        "Conservative",
        "Capital Preservation with Dividend Income - Focus on stable, dividend-paying blue-chip stocks with low volatility",
        7.0,   # target return %
        12.0,  # max volatility %
        0.6,   # beta min
        0.9,   # beta max
        50_000_000.0,  # aum_usd
        35,    # num_holdings (estimated)
        None,  # portfolio_beta (will be calculated in analytics)
        None,  # portfolio_volatility_pct (will be calculated in analytics)
        datetime.now().date(),  # created_date
        datetime.now()  # last_updated_at
    ),
    (
        "PM002",
        "Rena Tang",
        "Balanced",
        "Growth and Income with Value Focus - Balanced approach combining growth potential with income generation",
        11.0,  # target return %
        16.0,  # max volatility %
        0.9,   # beta min
        1.1,   # beta max
        75_000_000.0,  # aum_usd
        60,    # num_holdings (estimated)
        None,  # portfolio_beta
        None,  # portfolio_volatility_pct
        datetime.now().date(),
        datetime.now()
    ),
    (
        "PM003",
        "Mohit Arora",
        "Aggressive",
        "High-Growth Technology & Innovation - Concentrated positions in high-growth technology and innovation leaders",
        18.0,  # target return %
        28.0,  # max volatility %
        1.2,   # beta min
        1.8,   # beta max
        100_000_000.0,  # aum_usd
        45,    # num_holdings (estimated)
        None,  # portfolio_beta
        None,  # portfolio_volatility_pct
        datetime.now().date(),
        datetime.now()
    )
]

manager_schema = StructType([
    StructField("manager_id", StringType(), False),
    StructField("manager_name", StringType(), False),
    StructField("risk_profile", StringType(), False),
    StructField("strategy_description", StringType(), False),
    StructField("target_return_pct", DoubleType(), False),
    StructField("max_volatility_pct", DoubleType(), False),
    StructField("beta_min", DoubleType(), False),
    StructField("beta_max", DoubleType(), False),
    StructField("aum_usd", DoubleType(), True),  # NEW: Assets Under Management
    StructField("num_holdings", IntegerType(), True),  # NEW: Number of positions
    StructField("portfolio_beta", DoubleType(), True),  # NEW: Current portfolio beta
    StructField("portfolio_volatility_pct", DoubleType(), True),  # NEW: Current volatility
    StructField("created_date", DateType(), False),
    StructField("last_updated_at", TimestampType(), True)  # NEW: Last update timestamp
])

managers_df = spark.createDataFrame(managers_data, schema=manager_schema)

# Write to table
table_name = f"{catalog}.{schema}.portfolio_managers"
managers_df.write.mode("overwrite").saveAsTable(table_name)

print(f"✅ Created {table_name}")
print("\n👥 Portfolio Managers:")
managers_df.select("manager_name", "risk_profile", "target_return_pct", "max_volatility_pct").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Portfolio Holdings
# MAGIC
# MAGIC Creates realistic holdings for each manager based on their risk profile

# COMMAND ----------

print("💼 Generating Portfolio Holdings...")

def generate_portfolio_holdings(manager_id, risk_profile, num_stocks, total_value):
    """Generate holdings based on manager's risk profile"""
    
    # Get appropriate stocks for this risk profile
    if risk_profile == "Conservative":
        # Low beta stocks, focus on staples, utilities, healthcare
        sectors_query = """
        SELECT symbol, sector, beta, volatility_30d 
        FROM {}.{}company_universe 
        WHERE beta <= 1.0 
        AND sector IN ('Consumer Staples', 'Utilities', 'Healthcare', 'Financials')
        ORDER BY volatility_30d ASC
        LIMIT {}
        """.format(catalog, schema + ".", int(num_stocks * 0.7))
        
        # Add some medium risk for diversification
        sectors_query2 = """
        SELECT symbol, sector, beta, volatility_30d 
        FROM {}.{}.company_universe 
        WHERE beta > 0.9 AND beta <= 1.1
        AND sector IN ('Industrials', 'Technology')
        ORDER BY volatility_30d ASC
        LIMIT {}
        """.format(catalog, schema, int(num_stocks * 0.3))
        
    elif risk_profile == "Balanced":
        # Mix of low, medium, high beta
        sectors_query = """
        SELECT symbol, sector, beta, volatility_30d 
        FROM {}.{}.company_universe 
        WHERE beta >= 0.8 AND beta <= 1.3
        ORDER BY RAND()
        LIMIT {}
        """.format(catalog, schema, num_stocks)
        sectors_query2 = None
        
    else:  # Aggressive
        # High beta stocks, focus on tech and growth
        sectors_query = """
        SELECT symbol, sector, beta, volatility_30d 
        FROM {}.{}.company_universe 
        WHERE beta >= 1.1
        AND sector IN ('Technology', 'Consumer Discretionary', 'Communication Services')
        ORDER BY beta DESC
        LIMIT {}
        """.format(catalog, schema, int(num_stocks * 0.8))
        
        # Add some medium risk
        sectors_query2 = """
        SELECT symbol, sector, beta, volatility_30d 
        FROM {}.{}.company_universe 
        WHERE beta >= 0.9 AND beta < 1.1
        ORDER BY RAND()
        LIMIT {}
        """.format(catalog, schema, int(num_stocks * 0.2))
    
    # Get stocks
    stocks_df = spark.sql(sectors_query)
    if sectors_query2:
        stocks_df = stocks_df.union(spark.sql(sectors_query2))
    
    stocks = stocks_df.collect()
    
    # Generate weights (with some concentration for aggressive, more equal for conservative)
    if risk_profile == "Aggressive":
        # Power law distribution - some big positions
        weights = [random.uniform(0.5, 2.0) ** 2 for _ in stocks]
    elif risk_profile == "Conservative":
        # More equal distribution
        weights = [random.uniform(0.8, 1.2) for _ in stocks]
    else:
        # Balanced
        weights = [random.uniform(0.7, 1.5) for _ in stocks]
    
    # Normalize weights
    total_weight = sum(weights)
    normalized_weights = [w / total_weight for w in weights]
    
    # Create holdings
    holdings = []
    portfolio_id = f"{manager_id}_PORT"
    
    for stock, weight in zip(stocks, normalized_weights):
        value = total_value * weight
        # Estimate shares and prices (will be updated with real data later)
        estimated_price = 100.0  # Default price, will be replaced with real price
        estimated_shares = value / estimated_price
        
        holdings.append((
            portfolio_id,
            manager_id,
            stock.symbol,
            stock.sector,
            round(weight, 6),
            round(value, 2),
            datetime.now().date() - timedelta(days=random.randint(30, 365)),  # purchase_date
            datetime.now().date(),  # as_of_date
            round(estimated_shares, 4),  # shares (estimated)
            estimated_price,  # avg_cost_per_share (estimated)
            None,  # current_price (will be filled by ingestion)
            None,  # unrealized_gain_loss (will be calculated)
            None,  # unrealized_gain_loss_pct (will be calculated)
            stock.beta,  # beta from company universe
            stock.volatility_30d,  # volatility from company universe
            None  # last_price_update (will be filled by ingestion)
        ))
    
    return holdings

# Generate holdings for each manager
all_holdings = []

# Sarah Russel - Conservative (35 stocks, $50M)
print("\n1️⃣  Generating portfolio for Sarah Russel (Conservative)...")
sarah_holdings = generate_portfolio_holdings("PM001", "Conservative", 35, 50_000_000)
all_holdings.extend(sarah_holdings)
print(f"   ✅ Generated {len(sarah_holdings)} holdings")

# Rena Tang - Balanced (60 stocks, $75M)
print("\n2️⃣  Generating portfolio for Rena Tang (Balanced)...")
rena_holdings = generate_portfolio_holdings("PM002", "Balanced", 60, 75_000_000)
all_holdings.extend(rena_holdings)
print(f"   ✅ Generated {len(rena_holdings)} holdings")

# Mohit Arora - Aggressive (45 stocks, $100M)
print("\n3️⃣  Generating portfolio for Mohit Arora (Aggressive)...")
mohit_holdings = generate_portfolio_holdings("PM003", "Aggressive", 45, 100_000_000)
all_holdings.extend(mohit_holdings)
print(f"   ✅ Generated {len(mohit_holdings)} holdings")

# Create DataFrame
holdings_schema = StructType([
    StructField("portfolio_id", StringType(), False),
    StructField("manager_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("sector", StringType(), False),
    StructField("weight", DoubleType(), False),
    StructField("value_usd", DoubleType(), False),
    StructField("purchase_date", DateType(), False),
    StructField("as_of_date", DateType(), False),
    StructField("shares", DoubleType(), True),  # NEW: Number of shares
    StructField("avg_cost_per_share", DoubleType(), True),  # NEW: Average cost
    StructField("current_price", DoubleType(), True),  # NEW: Current market price
    StructField("unrealized_gain_loss", DoubleType(), True),  # NEW: P&L in USD
    StructField("unrealized_gain_loss_pct", DoubleType(), True),  # NEW: P&L %
    StructField("beta", DoubleType(), True),  # NEW: Stock beta
    StructField("volatility_30d", DoubleType(), True),  # NEW: Stock volatility
    StructField("last_price_update", TimestampType(), True)  # NEW: Price update timestamp
])

holdings_df = spark.createDataFrame(all_holdings, schema=holdings_schema)

# Write to table
table_name = f"{catalog}.{schema}.portfolio_holdings"
holdings_df.write.mode("overwrite").saveAsTable(table_name)

print(f"\n✅ Created {table_name}")
print(f"   Total holdings: {holdings_df.count()}")
print(f"   Total AUM: ${holdings_df.agg(F.sum('value_usd')).collect()[0][0]:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Portfolios Metadata Table

# COMMAND ----------

print("📋 Creating Portfolios Metadata...")

portfolios_data = [
    ("PM001_PORT", "PM001", "Sarah Russel - Conservative Growth", 50_000_000.0, 35, "Conservative", "S&P 500", datetime.now().date() - timedelta(days=730), datetime.now().date()),
    ("PM002_PORT", "PM002", "Rena Tang - Balanced Value", 75_000_000.0, 60, "Balanced", "S&P 500", datetime.now().date() - timedelta(days=550), datetime.now().date()),
    ("PM003_PORT", "PM003", "Mohit Arora - Aggressive Growth", 100_000_000.0, 45, "Aggressive", "NASDAQ-100", datetime.now().date() - timedelta(days=365), datetime.now().date())
]

portfolio_schema = StructType([
    StructField("portfolio_id", StringType(), False),
    StructField("manager_id", StringType(), False),
    StructField("portfolio_name", StringType(), False),
    StructField("total_value", DoubleType(), False),
    StructField("num_holdings", IntegerType(), False),
    StructField("risk_profile", StringType(), False),
    StructField("benchmark", StringType(), False),
    StructField("inception_date", DateType(), False),
    StructField("last_rebalance_date", DateType(), False)
])

portfolios_df = spark.createDataFrame(portfolios_data, schema=portfolio_schema)

# Write to table
table_name = f"{catalog}.{schema}.portfolios"
portfolios_df.write.mode("overwrite").saveAsTable(table_name)

print(f"✅ Created {table_name}")
print("\n📊 Portfolio Summary:")
portfolios_df.select("portfolio_name", "total_value", "num_holdings", "risk_profile").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary and Validation

# COMMAND ----------

print("=" * 80)
print("✅ MULTI-MANAGER PORTFOLIO SETUP COMPLETE")
print("=" * 80)

print("\n📊 Data Model Summary:")
print(f"\n1. Company Universe: {catalog}.{schema}.company_universe")
spark.sql(f"SELECT COUNT(*) as companies, COUNT(DISTINCT sector) as sectors FROM {catalog}.{schema}.company_universe").show()

print(f"\n2. Portfolio Managers: {catalog}.{schema}.portfolio_managers")
spark.sql(f"SELECT manager_name, risk_profile, target_return_pct FROM {catalog}.{schema}.portfolio_managers").show(truncate=False)

print(f"\n3. Portfolios: {catalog}.{schema}.portfolios")
spark.sql(f"SELECT portfolio_name, total_value, num_holdings FROM {catalog}.{schema}.portfolios").show(truncate=False)

print(f"\n4. Portfolio Holdings: {catalog}.{schema}.portfolio_holdings")
spark.sql(f"""
    SELECT 
        h.manager_id,
        m.manager_name,
        COUNT(*) as num_stocks,
        ROUND(SUM(h.value_usd), 0) as total_value,
        COUNT(DISTINCT h.sector) as num_sectors
    FROM {catalog}.{schema}.portfolio_holdings h
    JOIN {catalog}.{schema}.portfolio_managers m ON h.manager_id = m.manager_id
    GROUP BY h.manager_id, m.manager_name
    ORDER BY h.manager_id
""").show(truncate=False)

print("\n" + "=" * 80)
print("🚀 Ready for Agent Bricks Demo!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries for Demo

# COMMAND ----------

# Query 1: Show holdings for Sarah Russel
print("📊 Sample Query 1: Sarah Russel's Top 10 Holdings")
spark.sql(f"""
    SELECT 
        h.symbol,
        c.company_name,
        h.sector,
        c.beta,
        ROUND(h.weight * 100, 2) as weight_pct,
        ROUND(h.value_usd, 0) as value_usd
    FROM {catalog}.{schema}.portfolio_holdings h
    JOIN {catalog}.{schema}.company_universe c ON h.symbol = c.symbol
    WHERE h.manager_id = 'PM001'
    ORDER BY h.value_usd DESC
    LIMIT 10
""").show(truncate=False)

# Query 2: Compare sector allocations
print("\n📊 Sample Query 2: Sector Allocation Comparison")
spark.sql(f"""
    SELECT 
        m.manager_name,
        h.sector,
        COUNT(*) as num_stocks,
        ROUND(SUM(h.weight) * 100, 1) as weight_pct
    FROM {catalog}.{schema}.portfolio_holdings h
    JOIN {catalog}.{schema}.portfolio_managers m ON h.manager_id = m.manager_id
    GROUP BY m.manager_name, h.sector
    HAVING SUM(h.weight) > 0.05
    ORDER BY m.manager_name, weight_pct DESC
""").show(50, truncate=False)

# Query 3: Risk profile summary
print("\n📊 Sample Query 3: Portfolio Risk Metrics")
spark.sql(f"""
    SELECT 
        m.manager_name,
        m.risk_profile,
        ROUND(AVG(c.beta), 2) as avg_beta,
        ROUND(AVG(c.volatility_30d), 2) as avg_volatility,
        ROUND(MIN(c.beta), 2) as min_beta,
        ROUND(MAX(c.beta), 2) as max_beta
    FROM {catalog}.{schema}.portfolio_holdings h
    JOIN {catalog}.{schema}.portfolio_managers m ON h.manager_id = m.manager_id
    JOIN {catalog}.{schema}.company_universe c ON h.symbol = c.symbol
    GROUP BY m.manager_name, m.risk_profile
    ORDER BY m.manager_name
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

