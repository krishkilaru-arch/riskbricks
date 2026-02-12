"""
Multi-Manager Portfolio Generation for RiskBricks
=================================================

Creates realistic portfolios for three portfolio managers with different risk profiles:
- Sarah Russel: Conservative Growth
- Rena Tang: Balanced Value
- Mohit Arora: Aggressive Growth
"""

from fortune_500_portfolio import FORTUNE_500_COMPANIES
import random
from datetime import datetime, timedelta

# Portfolio Manager Profiles
PORTFOLIO_MANAGERS = [
    {
        "manager_id": "PM001",
        "manager_name": "Sarah Russel",
        "risk_profile": "Conservative",
        "strategy": "Capital Preservation with Dividend Income",
        "target_return_pct": 7.0,
        "max_volatility_pct": 12.0,
        "beta_min": 0.6,
        "beta_max": 0.9,
        "sector_preferences": {
            "Consumer Staples": 0.25,
            "Utilities": 0.20,
            "Healthcare": 0.20,
            "Financials": 0.15,
            "Industrials": 0.10,
            "Technology": 0.10
        },
        "num_holdings": 35,
        "total_value": 50_000_000.0  # $50M portfolio
    },
    {
        "manager_id": "PM002",
        "manager_name": "Rena Tang",
        "risk_profile": "Balanced",
        "strategy": "Growth and Income with Value Focus",
        "target_return_pct": 11.0,
        "max_volatility_pct": 16.0,
        "beta_min": 0.9,
        "beta_max": 1.1,
        "sector_preferences": {
            "Technology": 0.30,
            "Financials": 0.20,
            "Healthcare": 0.20,
            "Consumer Discretionary": 0.15,
            "Industrials": 0.10,
            "Communication Services": 0.05
        },
        "num_holdings": 60,
        "total_value": 75_000_000.0  # $75M portfolio
    },
    {
        "manager_id": "PM003",
        "manager_name": "Mohit Arora",
        "risk_profile": "Aggressive",
        "strategy": "High-Growth Technology & Innovation",
        "target_return_pct": 18.0,
        "max_volatility_pct": 28.0,
        "beta_min": 1.2,
        "beta_max": 1.8,
        "sector_preferences": {
            "Technology": 0.50,
            "Consumer Discretionary": 0.20,
            "Communication Services": 0.15,
            "Healthcare": 0.10,
            "Financials": 0.05
        },
        "num_holdings": 45,
        "total_value": 100_000_000.0  # $100M portfolio
    }
]

# Stock risk profiles (simplified beta estimates)
STOCK_RISK_PROFILES = {
    # Low Risk (Beta 0.5-0.9)
    "low_risk": [
        "PG", "KO", "PEP", "WMT", "COST", "JNJ", "MRK", "PFE", "ABT", "LLY",
        "NEE", "DUK", "SO", "D", "AEP", "JNJ", "UNH", "CVS", "CI",
        "JPM", "BAC", "WFC", "V", "MA", "MMM", "CAT", "HON", "UPS"
    ],
    # Medium Risk (Beta 0.9-1.2)
    "medium_risk": [
        "AAPL", "MSFT", "JPM", "BAC", "GS", "MS", "BLK", "SCHW",
        "UNH", "CVS", "HD", "LOW", "MCD", "SBUX", "NKE", "TGT",
        "BA", "RTX", "LMT", "UNP", "XOM", "CVX", "COP",
        "DIS", "CMCSA", "VZ", "T"
    ],
    # High Risk (Beta 1.2+)
    "high_risk": [
        "NVDA", "AMD", "TSLA", "META", "NFLX", "GOOGL", "AMZN",
        "CRM", "NOW", "ADBE", "INTC", "QCOM", "AVGO", "ORCL",
        "PYPL", "SQ", "SHOP", "SNOW", "DDOG", "NET",
        "ZM", "DOCU", "OKTA", "CRWD", "ZS"
    ]
}


def get_stock_sector(symbol):
    """Get sector for a stock symbol"""
    for sym, name, sector, industry in FORTUNE_500_COMPANIES:
        if sym == symbol:
            return sector
    return "Other"


def generate_manager_portfolio(manager_profile):
    """
    Generate a realistic portfolio for a given manager profile
    """
    manager_id = manager_profile["manager_id"]
    manager_name = manager_profile["manager_name"]
    risk_profile = manager_profile["risk_profile"]
    sector_prefs = manager_profile["sector_preferences"]
    num_holdings = manager_profile["num_holdings"]
    total_value = manager_profile["total_value"]
    
    # Select stocks based on risk profile
    if risk_profile == "Conservative":
        stock_pool = STOCK_RISK_PROFILES["low_risk"] + STOCK_RISK_PROFILES["medium_risk"][:10]
        random.shuffle(stock_pool)
        selected_stocks = stock_pool[:num_holdings]
    elif risk_profile == "Balanced":
        stock_pool = (
            STOCK_RISK_PROFILES["low_risk"][:15] +
            STOCK_RISK_PROFILES["medium_risk"] +
            STOCK_RISK_PROFILES["high_risk"][:10]
        )
        random.shuffle(stock_pool)
        selected_stocks = stock_pool[:num_holdings]
    else:  # Aggressive
        stock_pool = (
            STOCK_RISK_PROFILES["medium_risk"][:15] +
            STOCK_RISK_PROFILES["high_risk"]
        )
        random.shuffle(stock_pool)
        selected_stocks = stock_pool[:num_holdings]
    
    # Group stocks by sector
    stocks_by_sector = {}
    for symbol in selected_stocks:
        sector = get_stock_sector(symbol)
        if sector not in stocks_by_sector:
            stocks_by_sector[sector] = []
        stocks_by_sector[sector].append(symbol)
    
    # Allocate weights based on sector preferences
    holdings = []
    for sector, target_weight in sector_prefs.items():
        if sector in stocks_by_sector:
            stocks_in_sector = stocks_by_sector[sector]
            # Distribute sector weight across stocks
            if len(stocks_in_sector) > 0:
                # Slightly randomize within sector
                weights = []
                for _ in stocks_in_sector:
                    weights.append(random.uniform(0.8, 1.2))
                total_weights = sum(weights)
                normalized_weights = [w / total_weights for w in weights]
                
                for stock, norm_weight in zip(stocks_in_sector, normalized_weights):
                    stock_weight = target_weight * norm_weight
                    stock_value = total_value * stock_weight
                    holdings.append({
                        "portfolio_id": f"{manager_id}_PORT",
                        "manager_id": manager_id,
                        "symbol": stock,
                        "sector": sector,
                        "weight": round(stock_weight, 6),
                        "value_usd": round(stock_value, 2),
                        "shares": None,  # Will be calculated from price data
                        "purchase_date": (datetime.now() - timedelta(days=random.randint(30, 365))).date(),
                        "as_of_date": datetime.now().date()
                    })
    
    # Normalize weights to sum to 1.0
    total_weight = sum(h["weight"] for h in holdings)
    for h in holdings:
        h["weight"] = h["weight"] / total_weight
        h["value_usd"] = total_value * h["weight"]
    
    return holdings


def generate_all_portfolios():
    """
    Generate portfolios for all three managers
    """
    all_holdings = []
    
    for manager in PORTFOLIO_MANAGERS:
        print(f"\n{'='*60}")
        print(f"Generating portfolio for {manager['manager_name']}")
        print(f"Risk Profile: {manager['risk_profile']}")
        print(f"Target Holdings: {manager['num_holdings']}")
        print(f"Total Value: ${manager['total_value']:,.0f}")
        print(f"{'='*60}")
        
        holdings = generate_manager_portfolio(manager)
        all_holdings.extend(holdings)
        
        # Summary
        print(f"\n✅ Generated {len(holdings)} holdings")
        print(f"   Total Weight: {sum(h['weight'] for h in holdings):.4f}")
        print(f"   Total Value: ${sum(h['value_usd'] for h in holdings):,.2f}")
        
        # Sector breakdown
        sector_summary = {}
        for h in holdings:
            sector = h["sector"]
            if sector not in sector_summary:
                sector_summary[sector] = {"count": 0, "weight": 0, "value": 0}
            sector_summary[sector]["count"] += 1
            sector_summary[sector]["weight"] += h["weight"]
            sector_summary[sector]["value"] += h["value_usd"]
        
        print(f"\n   Sector Allocation:")
        for sector in sorted(sector_summary.keys(), key=lambda x: sector_summary[x]["weight"], reverse=True):
            s = sector_summary[sector]
            print(f"     {sector:25s}: {s['count']:2d} stocks, {s['weight']*100:5.1f}%, ${s['value']:12,.0f}")
    
    return all_holdings, PORTFOLIO_MANAGERS


def get_company_universe():
    """
    Get the complete company universe with metadata
    """
    companies = []
    for symbol, name, sector, industry in FORTUNE_500_COMPANIES:
        # Assign risk category
        if symbol in STOCK_RISK_PROFILES["low_risk"]:
            beta = round(random.uniform(0.6, 0.9), 2)
            volatility = round(random.uniform(10, 15), 2)
        elif symbol in STOCK_RISK_PROFILES["medium_risk"]:
            beta = round(random.uniform(0.9, 1.2), 2)
            volatility = round(random.uniform(15, 22), 2)
        elif symbol in STOCK_RISK_PROFILES["high_risk"]:
            beta = round(random.uniform(1.2, 1.8), 2)
            volatility = round(random.uniform(22, 35), 2)
        else:
            beta = round(random.uniform(0.8, 1.3), 2)
            volatility = round(random.uniform(12, 25), 2)
        
        companies.append({
            "symbol": symbol,
            "company_name": name,
            "sector": sector,
            "industry": industry,
            "beta": beta,
            "volatility_30d": volatility,
            "is_sp500": True,  # Assume all are S&P 500 for demo
            "is_fortune500": True
        })
    
    return companies


if __name__ == "__main__":
    print("🏗️  RiskBricks Multi-Manager Portfolio Generator")
    print("=" * 60)
    
    # Generate portfolios
    holdings, managers = generate_all_portfolios()
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETE: Generated {len(holdings)} total holdings across {len(managers)} managers")
    print(f"   Total AUM: ${sum(m['total_value'] for m in managers):,.0f}")
    print(f"={'='*60}\n")
    
    # Get company universe
    companies = get_company_universe()
    print(f"📊 Company Universe: {len(companies)} companies")
    
    print("\n✅ Ready to load into Databricks!")
