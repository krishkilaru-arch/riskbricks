"""RiskBricks — Static Fallback Constants

Used ONLY during initial setup (Phase 0-1) before gold.company_universe
is populated.  After that, all notebooks call the dynamic loaders in
config/__init__.py  (get_symbols, get_sector_map, get_company_names).
"""

# ── Fallback symbol list (51 stocks, pre-company_universe) ───────────
FALLBACK_SYMBOLS = [
    "LMT", "RTX", "NOC", "GD", "BA", "HII",
    "XOM", "CVX", "COP", "SLB", "HAL", "OXY",
    "JPM", "BAC", "GS", "MS", "C", "WFC",
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "INTC", "AMD", "AVGO", "QCOM", "MU", "LRCX", "AMAT",
    "WMT", "COST", "HD", "NKE", "MCD", "SBUX",
    "JNJ", "PFE", "UNH", "LLY", "ABBV", "MRK",
    "CAT", "DE", "HON", "GE", "MMM",
    "UAL", "DAL", "AAL",
]

# ── Fallback sector mapping (GICS-aligned names for company_universe) ─
FALLBACK_SECTOR_MAP = {
    "LMT": "Industrials", "RTX": "Industrials", "NOC": "Industrials",
    "GD": "Industrials", "BA": "Industrials", "HII": "Industrials",
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy",
    "SLB": "Energy", "HAL": "Energy", "OXY": "Energy",
    "JPM": "Financials", "BAC": "Financials", "GS": "Financials",
    "MS": "Financials", "C": "Financials", "WFC": "Financials",
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "AMZN": "Technology", "NVDA": "Technology", "META": "Technology",
    "TSLA": "Consumer Discretionary",
    "INTC": "Technology", "AMD": "Technology", "AVGO": "Technology",
    "QCOM": "Technology", "MU": "Technology", "LRCX": "Technology",
    "AMAT": "Technology",
    "WMT": "Consumer Staples", "COST": "Consumer Staples",
    "HD": "Consumer Discretionary", "NKE": "Consumer Discretionary",
    "MCD": "Consumer Discretionary", "SBUX": "Consumer Discretionary",
    "JNJ": "Healthcare", "PFE": "Healthcare", "UNH": "Healthcare",
    "LLY": "Healthcare", "ABBV": "Healthcare", "MRK": "Healthcare",
    "CAT": "Industrials", "DE": "Industrials", "HON": "Industrials",
    "GE": "Industrials", "MMM": "Industrials",
    "UAL": "Industrials", "DAL": "Industrials", "AAL": "Industrials",
}

# ── Company names for RSS feed scraping ───────────────────────────
COMPANY_NAMES = {
    "LMT": "Lockheed Martin", "RTX": "RTX Corp", "NOC": "Northrop Grumman",
    "GD": "General Dynamics", "BA": "Boeing", "HII": "Huntington Ingalls",
    "XOM": "ExxonMobil", "CVX": "Chevron", "COP": "ConocoPhillips",
    "SLB": "Schlumberger", "HAL": "Halliburton", "OXY": "Occidental Petroleum",
    "JPM": "JPMorgan Chase", "BAC": "Bank of America", "GS": "Goldman Sachs",
    "MS": "Morgan Stanley", "C": "Citigroup", "WFC": "Wells Fargo",
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet Google",
    "AMZN": "Amazon", "NVDA": "NVIDIA", "META": "Meta Platforms", "TSLA": "Tesla",
    "INTC": "Intel", "AMD": "Advanced Micro Devices", "AVGO": "Broadcom",
    "QCOM": "Qualcomm", "MU": "Micron", "LRCX": "Lam Research",
    "AMAT": "Applied Materials",
    "WMT": "Walmart", "COST": "Costco", "HD": "Home Depot",
    "NKE": "Nike", "MCD": "McDonald's", "SBUX": "Starbucks",
    "JNJ": "Johnson & Johnson", "PFE": "Pfizer", "UNH": "UnitedHealth",
    "LLY": "Eli Lilly", "ABBV": "AbbVie", "MRK": "Merck",
    "CAT": "Caterpillar", "DE": "John Deere", "HON": "Honeywell",
    "GE": "GE Aerospace", "MMM": "3M",
    "UAL": "United Airlines", "DAL": "Delta Air Lines", "AAL": "American Airlines",
}

# ── FRED macro indicator series ───────────────────────────────────
FRED_SERIES = {
    "VIXCLS": "VIX",
    "T10Y2Y": "Yield_Spread_10Y2Y",
    "DFF": "Fed_Funds_Rate",
    "BAMLH0A0HYM2": "HY_Credit_Spread",
    "DGS10": "Treasury_10Y",
    "DGS2": "Treasury_2Y",
    "DTWEXBGS": "USD_Index",
    "DCOILWTICO": "WTI_Oil",
}

# ── Portfolio manager names (for agent extraction) ───────────────
KNOWN_MANAGERS = ["Sarah Russel", "Rena Tang", "Mohit Arora"]


# ── Stress test scenarios (used by daily_data_refresh, create_risk_analytics) ─
STRESS_SCENARIOS = [
    {"name": "Market Crash (-20%)", "description": "S&P 500 drops 20%", "shock_pct": -20.0},
    {"name": "Rate Hike (+200bp)", "description": "Fed raises rates 200bp, equity drop ~8%", "shock_pct": -8.0},
    {"name": "Recession", "description": "GDP contracts, broad market selloff ~15%", "shock_pct": -15.0},
    {"name": "Bull Rally (+15%)", "description": "Strong market rally +15%", "shock_pct": 15.0},
]

# ── FRED indicator metadata (series title, units, frequency, seasonal adj) ────
FRED_INDICATOR_META = {
    "FEDFUNDS": ("Federal Funds Effective Rate", "Percent", "Monthly", "Seasonally Adjusted"),
    "CPIAUCSL": ("Consumer Price Index for All Urban Consumers", "Index 1982-1984=100", "Monthly", "Seasonally Adjusted"),
    "UNRATE":   ("Unemployment Rate", "Percent", "Monthly", "Seasonally Adjusted"),
    "GDP":      ("Gross Domestic Product", "Billions of Dollars", "Quarterly", "Seasonally Adjusted Annual Rate"),
    "DGS10":    ("10-Year Treasury Constant Maturity Rate", "Percent", "Daily", "Not Seasonally Adjusted"),
    "VIXCLS":   ("CBOE Volatility Index: VIX", "Index", "Daily", "Not Seasonally Adjusted"),
}

# ── GDELT company keyword overrides (supplements dynamic company_name tokens) ─
GDELT_COMPANY_KEYWORDS = {
    "AAPL": ["APPLE"], "MSFT": ["MICROSOFT"], "GOOGL": ["GOOGLE", "ALPHABET"],
    "AMZN": ["AMAZON"], "TSLA": ["TESLA"], "NVDA": ["NVIDIA"],
    "META": ["FACEBOOK"], "NFLX": ["NETFLIX"], "COST": ["COSTCO"],
    "JPM": ["JPMORGAN"], "BAC": ["BANK AMERICA"], "GS": ["GOLDMAN SACHS"],
    "MS": ["MORGAN STANLEY"], "WMT": ["WALMART"], "HD": ["HOME DEPOT"],
}
