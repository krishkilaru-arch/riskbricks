-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🎯 Ensure Top 20 Stocks in Company Universe
-- MAGIC
-- MAGIC Makes sure the top 20 stocks have entries in company_universe table

-- COMMAND ----------

-- Check which top 20 stocks are already in company_universe
SELECT 
    symbol, 
    company_name, 
    sector, 
    industry,
    beta,
    volatility_30d,
    is_sp500,
    is_fortune500
FROM riskbricks.gold.company_universe
WHERE symbol IN (
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'JPM', 'V', 'WMT',
    'JNJ', 'PG', 'MA', 'HD', 'BAC',
    'XOM', 'CVX', 'KO', 'DIS', 'NFLX'
)
ORDER BY symbol;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Column Mapping for INSERT
-- MAGIC
-- MAGIC ```
-- MAGIC symbol, company_name, sector, industry, beta, volatility_30d,
-- MAGIC is_sp500, is_fortune500, 
-- MAGIC volatility_90d, avg_volume_30d, market_cap_usd,
-- MAGIC latest_price, price_change_1d_pct, price_change_1w_pct, price_change_1m_pct,
-- MAGIC dividend_yield, pe_ratio, 
-- MAGIC country, currency, 
-- MAGIC computed_at, price_updated_at
-- MAGIC ```
-- MAGIC
-- MAGIC **Note:** NULL values will be filled by subsequent data ingestion jobs.

-- COMMAND ----------

-- Insert any missing top 20 stocks
MERGE INTO riskbricks.gold.company_universe AS target
USING (
    SELECT 
        symbol, company_name, sector, industry, beta, volatility_30d
    FROM VALUES
        ('AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics', 1.2, 18.5),
        ('MSFT', 'Microsoft Corporation', 'Technology', 'Software', 1.0, 16.2),
        ('GOOGL', 'Alphabet Inc.', 'Technology', 'Internet', 1.1, 19.8),
        ('AMZN', 'Amazon.com Inc.', 'Technology', 'Internet Retail', 1.3, 22.4),
        ('NVDA', 'NVIDIA Corporation', 'Technology', 'Semiconductors', 1.7, 32.1),
        ('META', 'Meta Platforms Inc.', 'Technology', 'Social Media', 1.4, 28.3),
        ('TSLA', 'Tesla Inc.', 'Consumer Discretionary', 'Automobiles', 1.8, 35.7),
        ('JPM', 'JPMorgan Chase & Co.', 'Financials', 'Banks', 1.0, 17.2),
        ('V', 'Visa Inc.', 'Financials', 'Payment Processing', 0.9, 16.7),
        ('WMT', 'Walmart Inc.', 'Consumer Staples', 'Discount Stores', 0.7, 12.3),
        ('JNJ', 'Johnson & Johnson', 'Healthcare', 'Pharmaceuticals', 0.7, 12.1),
        ('PG', 'Procter & Gamble Co.', 'Consumer Staples', 'Household Products', 0.6, 11.2),
        ('MA', 'Mastercard Inc.', 'Financials', 'Payment Processing', 0.9, 16.4),
        ('HD', 'Home Depot Inc.', 'Consumer Discretionary', 'Home Improvement', 0.9, 17.3),
        ('BAC', 'Bank of America Corp.', 'Financials', 'Banks', 1.1, 19.4),
        ('XOM', 'Exxon Mobil Corporation', 'Energy', 'Oil & Gas', 1.1, 24.6),
        ('CVX', 'Chevron Corporation', 'Energy', 'Oil & Gas', 1.0, 22.1),
        ('KO', 'Coca-Cola Company', 'Consumer Staples', 'Beverages', 0.6, 11.8),
        ('DIS', 'Walt Disney Company', 'Communication Services', 'Entertainment', 1.1, 20.7),
        ('NFLX', 'Netflix Inc.', 'Communication Services', 'Entertainment', 1.3, 27.4)
    AS t(symbol, company_name, sector, industry, beta, volatility_30d)
) source
ON target.symbol = source.symbol
WHEN MATCHED THEN
    UPDATE SET
        company_name = source.company_name,
        sector = source.sector,
        industry = source.industry,
        beta = source.beta,
        volatility_30d = source.volatility_30d,
        is_sp500 = true,
        is_fortune500 = true,
        computed_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (
        symbol, company_name, sector, industry, beta, volatility_30d, 
        is_sp500, is_fortune500, volatility_90d, avg_volume_30d, market_cap_usd,
        latest_price, price_change_1d_pct, price_change_1w_pct, price_change_1m_pct,
        dividend_yield, pe_ratio, country, currency, computed_at, price_updated_at
    )
    VALUES (
        source.symbol, 
        source.company_name, 
        source.sector, 
        source.industry, 
        source.beta, 
        source.volatility_30d,
        true,
        true,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'USA',
        'USD',
        current_timestamp(),
        NULL
    );

-- COMMAND ----------

-- Verify all top 20 are now present
SELECT COUNT(*) as top20_count
FROM riskbricks.gold.company_universe
WHERE symbol IN (
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'JPM', 'V', 'WMT',
    'JNJ', 'PG', 'MA', 'HD', 'BAC',
    'XOM', 'CVX', 'KO', 'DIS', 'NFLX'
);

-- Should return 20

-- COMMAND ----------

-- Show them all with details
SELECT 
    symbol, 
    company_name, 
    sector, 
    industry,
    beta,
    volatility_30d,
    is_sp500,
    is_fortune500,
    country,
    currency
FROM riskbricks.gold.company_universe
WHERE symbol IN (
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'JPM', 'V', 'WMT',
    'JNJ', 'PG', 'MA', 'HD', 'BAC',
    'XOM', 'CVX', 'KO', 'DIS', 'NFLX'
)
ORDER BY symbol;

-- COMMAND ----------


