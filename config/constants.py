"""
RiskBricks — Shared Constants
==============================
Single source of truth for sector maps, focus symbols, and other shared data.
Imported by notebooks, app pages, and agent code.
"""

# Focus symbols for ML predictions and news scraping
FOCUS_SYMBOLS = [
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

# Sector mapping for all focus symbols
SECTOR_MAP = {
    "LMT": "Defense", "RTX": "Defense", "NOC": "Defense", "GD": "Defense",
    "BA": "Defense", "HII": "Defense",
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "SLB": "Energy",
    "HAL": "Energy", "OXY": "Energy",
    "JPM": "Banks", "BAC": "Banks", "GS": "Banks", "MS": "Banks",
    "C": "Banks", "WFC": "Banks",
    "AAPL": "BigTech", "MSFT": "BigTech", "GOOGL": "BigTech", "AMZN": "BigTech",
    "NVDA": "BigTech", "META": "BigTech", "TSLA": "BigTech",
    "INTC": "Semis", "AMD": "Semis", "AVGO": "Semis", "QCOM": "Semis",
    "MU": "Semis", "LRCX": "Semis", "AMAT": "Semis",
    "WMT": "Consumer", "COST": "Consumer", "HD": "Consumer", "NKE": "Consumer",
    "MCD": "Consumer", "SBUX": "Consumer",
    "JNJ": "Pharma", "PFE": "Pharma", "UNH": "Pharma", "LLY": "Pharma",
    "ABBV": "Pharma", "MRK": "Pharma",
    "CAT": "Industrial", "DE": "Industrial", "HON": "Industrial",
    "GE": "Industrial", "MMM": "Industrial",
    "UAL": "Airlines", "DAL": "Airlines", "AAL": "Airlines",
}

# Company names for RSS feed scraping
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
    "QCOM": "Qualcomm", "MU": "Micron", "LRCX": "Lam Research", "AMAT": "Applied Materials",
    "WMT": "Walmart", "COST": "Costco", "HD": "Home Depot",
    "NKE": "Nike", "MCD": "McDonald's", "SBUX": "Starbucks",
    "JNJ": "Johnson & Johnson", "PFE": "Pfizer", "UNH": "UnitedHealth",
    "LLY": "Eli Lilly", "ABBV": "AbbVie", "MRK": "Merck",
    "CAT": "Caterpillar", "DE": "John Deere", "HON": "Honeywell",
    "GE": "GE Aerospace", "MMM": "3M",
    "UAL": "United Airlines", "DAL": "Delta Air Lines", "AAL": "American Airlines",
}

# FRED macro indicator series
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

# Portfolio manager names (for agent extraction)
KNOWN_MANAGERS = ["Sarah Russel", "Rena Tang", "Mohit Arora"]

# ML model features
CURATED_FEATURES = [
    "return_5d", "return_20d", "volatility_20d",
    "ai_sentiment", "gdelt_tone", "advance_ratio",
    "sector_momentum_5d", "sector_breadth",
    "rsi_14", "macd_hist", "bb_pct", "volume_ratio",
    "gap_pct", "close_position",
    "vix", "hy_spread", "yield_spread",
]

ALL_SECTORS = sorted(set(SECTOR_MAP.values()))
