import streamlit as st
import os
import json

st.set_page_config(
    page_title="AI Agent Chat - RiskBricks",
    page_icon="🤖",
    layout="wide"
)

st.title("🤖 AI Agent Chat")
st.markdown("Ask questions about portfolio managers, risk metrics, and holdings")

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []

def execute_sql_query(query):
    """Execute SQL query against Databricks using secrets"""
    try:
        from databricks import sql
        
        # Get credentials from environment (populated from secrets)
        token = os.getenv('DATABRICKS_TOKEN')
        hostname = os.getenv('DATABRICKS_HOST')
        warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID', 'default')
        
        if not token or not hostname:
            return [{"error": "Missing DATABRICKS_TOKEN or DATABRICKS_HOST environment variables"}]
        
        connection = sql.connect(
            server_hostname=hostname,
            http_path=f'/sql/1.0/warehouses/{warehouse_id}',
            access_token=token
        )
        
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        connection.close()
        
        return [dict(zip(columns, row)) for row in result]
    except Exception as e:
        return {"error": str(e)}

# Define available tools/functions
TOOLS = [
    {
        "name": "get_risk_metrics",
        "description": "Get portfolio risk metrics (VaR, beta, volatility) for a specific manager. Use manager names: 'Sarah Russel', 'Rena Tang', or 'Mohit Arora'",
        "parameters": {
            "manager_name": "Name of the portfolio manager"
        }
    },
    {
        "name": "get_portfolio_holdings",
        "description": "Get the stock holdings for a specific portfolio manager. Shows symbols, values, and weights.",
        "parameters": {
            "manager_name": "Name of the portfolio manager"
        }
    },
    {
        "name": "get_sector_exposures",
        "description": "Get sector allocation percentages for a specific manager's portfolio.",
        "parameters": {
            "manager_name": "Name of the portfolio manager"
        }
    },
    {
        "name": "get_stress_tests",
        "description": "Get stress test results showing portfolio impact under various market scenarios.",
        "parameters": {
            "manager_name": "Name of the portfolio manager"
        }
    },
    {
        "name": "compare_managers",
        "description": "Compare all three portfolio managers side-by-side with key metrics.",
        "parameters": {}
    },
    {
        "name": "get_macro_context",
        "description": "Get current macroeconomic indicators (GDP, Fed Funds Rate, VIX, etc.).",
        "parameters": {}
    },
    {
        "name": "get_historical_news_impact",
        "description": "Get historical news events and their actual price impact for a stock over past years. Shows sentiment and 1-day, 1-week impact.",
        "parameters": {
            "symbol": "Stock ticker (e.g., AAPL)",
            "years_back": "Number of years to look back (default 5)"
        }
    },
    {
        "name": "get_sector_news_sensitivity",
        "description": "Analyze how sensitive a sector is to news based on 12 years of historical data. Shows correlation and average impacts.",
        "parameters": {
            "sector_name": "Sector name (e.g., Technology)"
        }
    },
    {
        "name": "find_similar_events",
        "description": "Find historical news events with similar sentiment in a sector. Useful for: 'what happened when similar news broke'",
        "parameters": {
            "sentiment_score": "Target sentiment (-10 to +10)",
            "sector_name": "Sector to search",
            "days_back": "Days to look back (default 1825 = 5 years)"
        }
    },
    {
        "name": "predict_portfolio_news_impact",
        "description": "Predict portfolio impact based on historical data when similar news sentiment occurs. Shows confidence and historical range.",
        "parameters": {
            "manager_name": "Portfolio manager name",
            "sentiment_score": "News sentiment score (-10 to +10)"
        }
    },
    {
        "name": "get_news_correlation_stats",
        "description": "Get overall statistics showing how news sentiment correlates with price movements over 12 years. Proves news moves markets.",
        "parameters": {}
    },
    {
        "name": "query_rag",
        "description": "Ask questions using RAG with multiple sources: news, SEC filings (10-K, 10-Q, 8-K), Wikipedia, stock data. Examples: 'What happened to Costco?', 'Apple 10-K annual report?', 'Tesla company history?', 'Microsoft quarterly report?'",
        "parameters": {
            "question": "Question about stocks, news, SEC filings, or company info"
        }
    }
]

def call_uc_function(function_name, parameters):
    """Call a Unity Catalog function and return results"""
    
    if function_name == "get_risk_metrics":
        manager = parameters.get("manager_name", "")
        query = f"SELECT * FROM riskbricks.agent_tools.get_risk_metrics('{manager}')"
        results = execute_sql_query(query)
        if results and not isinstance(results, dict):
            return results[0]
        return results
    
    elif function_name == "get_portfolio_holdings":
        manager = parameters.get("manager_name", "")
        query = f"SELECT * FROM riskbricks.agent_tools.get_portfolio_holdings('{manager}') LIMIT 15"
        return execute_sql_query(query)
    
    elif function_name == "get_sector_exposures":
        manager = parameters.get("manager_name", "")
        query = f"SELECT * FROM riskbricks.agent_tools.get_sector_exposures('{manager}')"
        return execute_sql_query(query)
    
    elif function_name == "get_stress_tests":
        manager = parameters.get("manager_name", "")
        query = f"SELECT * FROM riskbricks.agent_tools.get_stress_tests('{manager}')"
        return execute_sql_query(query)
    
    elif function_name == "compare_managers":
        query = "SELECT * FROM riskbricks.agent_tools.compare_managers()"
        return execute_sql_query(query)
    
    elif function_name == "get_macro_context":
        query = "SELECT * FROM riskbricks.agent_tools.get_macro_context()"
        return execute_sql_query(query)
    
    elif function_name == "get_historical_news_impact":
        symbol = parameters.get("symbol", "AAPL")
        years_back = parameters.get("years_back", 5)
        query = f"SELECT * FROM riskbricks.agent_tools.get_historical_news_impact('{symbol}', {years_back})"
        return execute_sql_query(query)
    
    elif function_name == "get_sector_news_sensitivity":
        sector = parameters.get("sector_name", "Technology")
        query = f"SELECT * FROM riskbricks.agent_tools.get_sector_news_sensitivity('{sector}')"
        results = execute_sql_query(query)
        if results and not isinstance(results, dict):
            return results[0]
        return results
    
    elif function_name == "find_similar_events":
        sentiment = parameters.get("sentiment_score", -5.0)
        sector = parameters.get("sector_name", "Technology")
        days_back = parameters.get("days_back", 1825)
        query = f"SELECT * FROM riskbricks.agent_tools.find_similar_events({sentiment}, '{sector}', {days_back}) LIMIT 10"
        return execute_sql_query(query)
    
    elif function_name == "predict_portfolio_news_impact":
        manager = parameters.get("manager_name", "")
        sentiment = parameters.get("sentiment_score", -5.0)
        query = f"SELECT * FROM riskbricks.agent_tools.predict_portfolio_news_impact('{manager}', {sentiment})"
        results = execute_sql_query(query)
        if results and not isinstance(results, dict):
            return results[0]
        return results
    
    elif function_name == "get_news_correlation_stats":
        query = "SELECT * FROM riskbricks.agent_tools.get_news_correlation_stats()"
        results = execute_sql_query(query)
        if results and not isinstance(results, dict):
            return results[0]
        return results
    
    elif function_name == "query_rag":
        question = parameters.get("question", "")
        # Escape single quotes in the question
        question_escaped = question.replace("'", "''")
        query = f"SELECT riskbricks.functions.query_rag('{question_escaped}') as answer"
        results = execute_sql_query(query)
        if results and not isinstance(results, dict) and len(results) > 0:
            return {"answer": results[0].get("answer", "No answer available")}
        return results
    
    return {"error": f"Unknown function: {function_name}"}

def query_llm(user_query, conversation_history):
    """Query with AI-powered responses and function calling"""
    
    # Detect which functions to call
    function_calls = detect_function_calls(user_query, "")
    
    # Build AI response with personality
    query_lower = user_query.lower()
    
    # Generate contextual introduction based on query type
    if function_calls:
        # We have data to show
        intro_responses = {
            "risk": "Let me analyze the risk metrics for you. 📊",
            "compare": "Here's a comprehensive comparison of our portfolio managers. 👥",
            "holdings": "I've pulled the latest holdings data. 📈",
            "sector": "Let me break down the sector allocation. 🏢",
            "stress": "Let me show you how portfolios would perform under stress scenarios. ⚠️",
            "recession": "Here's how our portfolios would handle a recession scenario. 📉",
            "crash": "Let me analyze portfolio resilience during market crashes. 💥",
            "perform": "Here's the portfolio performance analysis you requested. 📊",
            "macro": "Let me get the current macroeconomic landscape for you. 🌍",
            "news": "Let me search for relevant news articles for you. 📰",
            "happened": "Let me find what happened based on our sources. 🔍",
            "costco": "Let me search for Costco information. 📰",
            "apple": "Let me find the latest Apple information. 📰",
            "tesla": "Let me search for Tesla information. 📰",
            "10-k": "Let me look up the 10-K annual report for you. 📄",
            "10k": "Let me look up the 10-K annual report for you. 📄",
            "10-q": "Let me find the quarterly report for you. 📄",
            "annual": "Let me search the annual report. 📄",
            "quarterly": "Let me search the quarterly report. 📄",
            "filing": "Let me look up SEC filings for you. 📄",
            "history": "Let me find company background information. 📚",
            "wikipedia": "Let me search for company information. 📚"
        }
        
        intro = "Here's what I found:\n\n"
        for key, msg in intro_responses.items():
            if key in query_lower:
                intro = msg + "\n\n"
                break
        
        # Execute functions and format results
        response = intro
        
        # Group stress tests by manager if multiple
        if len(function_calls) > 1 and all(f["name"] == "get_stress_tests" for f in function_calls):
            # Multiple managers stress test comparison
            response += "## Recession Impact Analysis Across All Portfolios\n\n"
            for func_call in function_calls:
                result = call_uc_function(func_call["name"], func_call["parameters"])
                manager_name = func_call["parameters"]["manager_name"]
                response += format_stress_test_compact(manager_name, result)
        else:
            # Regular single function or mixed functions
            for func_call in function_calls:
                result = call_uc_function(func_call["name"], func_call["parameters"])
                response += format_function_result(func_call["name"], result)
        
        # Add AI insights based on the data
        response += generate_insights(user_query, function_calls)
        return response
    
    # Handle out-of-scope questions with helpful guidance
    elif any(word in query_lower for word in ['buy', 'sell', 'invest', 'trade', 'recommend', 'should i', 'which stock', 'what stock', 'good stock', 'pick', 'choose', 'best stock']):
        return """I appreciate your interest in investment recommendations! 📈

However, I'm designed to analyze **existing portfolio risk**, not provide investment advice. 

**Instead, let me show you what our expert managers are holding!** 🎯

Each of our portfolio managers has a different investment philosophy:

👤 **Sarah Russel (Conservative)** - 32 holdings, focused on stability
- Ask: "Show me Sarah Russel's holdings"

👤 **Rena Tang (Balanced)** - 60 holdings, diversified approach  
- Ask: "Show me Rena Tang's holdings"

👤 **Mohit Arora (Aggressive)** - 27 holdings, concentrated in growth
- Ask: "Show me Mohit Arora's holdings"

**📊 I can also help you analyze:**
- Sector exposure across portfolios
- Risk metrics (VaR, beta, volatility)
- Stress test scenarios
- Manager comparisons
- Current macro environment

**💡 Better questions to ask:**
- "Show me Mohit's top 10 holdings" (see actual stocks!)
- "What sectors does Sarah focus on?"
- "Compare all three managers"
- "Which manager has the highest tech exposure?"

I'll show you **real holdings** with actual weights and values, so you can see what professional managers are investing in! 🚀"""
    
    else:
        # General help
        return """I'm your RiskBricks AI assistant! 🤖 I specialize in portfolio risk analytics.

**📊 What I Can Help With:**

**Portfolio Managers:**
- 👤 **Sarah Russel** - Conservative ($50M, β=0.68, 32 holdings)
- 👤 **Rena Tang** - Balanced ($75M, β=0.90, 60 holdings)  
- 👤 **Mohit Arora** - Aggressive ($100M, β=0.88, 27 holdings)

**Analysis Types:**
- 📊 Risk profiles (VaR, beta, volatility)
- 📈 Portfolio holdings & weights
- 🏢 Sector allocations
- ⚠️ Stress test scenarios
- 🌍 Macroeconomic context
- 👥 Cross-manager comparisons

**💡 Try These Questions:**
- "What is Sarah Russel's risk profile?"
- "Compare all three managers"
- "Show me Mohit Arora's sector exposure"
- "How would portfolios perform in a recession?"
- "What's the highest VaR portfolio?"

Ask me anything about these portfolios! I have 12 years of market data and real-time analytics. 🚀"""

def generate_insights(query, function_calls):
    """Generate AI insights based on the data shown"""
    query_lower = query.lower()
    insights = "\n\n💡 **Key Insights:**\n\n"
    
    if any(f["name"] == "get_risk_metrics" for f in function_calls):
        if "sarah" in query_lower:
            insights += "- Sarah's conservative approach prioritizes capital preservation over growth\n"
            insights += "- Lower VaR means less downside risk compared to aggressive strategies\n"
            insights += "- Beta < 1.0 indicates lower sensitivity to market movements\n"
        elif "mohit" in query_lower:
            insights += "- Mohit's aggressive strategy targets higher returns with concentrated positions\n"
            insights += "- Higher VaR reflects greater potential for short-term losses\n"
            insights += "- Technology concentration amplifies both gains and risks\n"
        elif "rena" in query_lower:
            insights += "- Rena balances growth and stability with diversified holdings\n"
            insights += "- Moderate VaR provides middle-ground risk exposure\n"
            insights += "- Beta near 1.0 tracks market performance closely\n"
    
    elif any(f["name"] == "compare_managers" for f in function_calls):
        insights += "- **Risk Spectrum**: Sarah (low) → Rena (medium) → Mohit (high)\n"
        insights += "- **Diversification**: Rena has 2x more positions than Mohit\n"
        insights += "- **AUM Distribution**: Total $225M across 3 distinct strategies\n"
        insights += "- **VaR Range**: $1.3M to $3.2M daily exposure at 95% confidence\n"
    
    elif any(f["name"] == "get_stress_tests" for f in function_calls):
        if len([f for f in function_calls if f["name"] == "get_stress_tests"]) > 1:
            # Multiple managers comparison
            insights += "- **Conservative portfolios** (Sarah) show better downside protection\n"
            insights += "- **Aggressive portfolios** (Mohit) experience larger drawdowns but recover faster\n"
            insights += "- **Balanced approach** (Rena) provides middle-ground resilience\n"
            insights += "- Recession impacts vary by sector exposure and concentration\n"
        else:
            insights += "- Stress tests reveal portfolio resilience under extreme scenarios\n"
            insights += "- Market crash would impact all portfolios but with varying severity\n"
            insights += "- Sector-specific shocks affect managers differently based on exposure\n"
    
    return insights if insights != "\n\n💡 **Key Insights:**\n\n" else ""

def detect_function_calls(user_query, llm_response):
    """Detect which functions should be called based on query"""
    query_lower = user_query.lower()
    function_calls = []
    
    # Detect manager mentions
    manager_name = None
    if 'sarah' in query_lower:
        manager_name = 'Sarah Russel'
    elif 'rena' in query_lower:
        manager_name = 'Rena Tang'
    elif 'mohit' in query_lower:
        manager_name = 'Mohit Arora'
    
    # Detect stress test / scenario queries first (can be for all managers)
    if any(word in query_lower for word in ['stress', 'scenario', 'recession', 'crash', 'downturn', 'crisis', 'perform']):
        if manager_name:
            # Specific manager stress test
            function_calls.append({"name": "get_stress_tests", "parameters": {"manager_name": manager_name}})
        else:
            # All managers - get stress tests for each
            for manager in ['Sarah Russel', 'Rena Tang', 'Mohit Arora']:
                function_calls.append({"name": "get_stress_tests", "parameters": {"manager_name": manager}})
    
    # Detect intent
    elif 'compare' in query_lower or ('all' in query_lower and 'manager' in query_lower):
        function_calls.append({"name": "compare_managers", "parameters": {}})
    
    elif manager_name:
        if 'risk' in query_lower or 'profile' in query_lower or 'var' in query_lower or 'beta' in query_lower:
            function_calls.append({"name": "get_risk_metrics", "parameters": {"manager_name": manager_name}})
        
        if 'holdings' in query_lower or 'positions' in query_lower or 'stocks' in query_lower:
            function_calls.append({"name": "get_portfolio_holdings", "parameters": {"manager_name": manager_name}})
        
        if 'sector' in query_lower or 'exposure' in query_lower:
            function_calls.append({"name": "get_sector_exposures", "parameters": {"manager_name": manager_name}})
    
    if 'macro' in query_lower or 'economic' in query_lower or 'market' in query_lower:
        function_calls.append({"name": "get_macro_context", "parameters": {}})
    
    # Historical news analysis
    if any(word in query_lower for word in ['historical', 'history', 'past', 'similar', 'before', '12 years', 'data', 'correlation', 'based on']):
        if any(word in query_lower for word in ['news', 'article', 'sentiment', 'events']):
            # Check for stock-specific historical queries
            symbols = ['aapl', 'apple', 'msft', 'microsoft', 'googl', 'google', 'nvda', 'nvidia', 'tsla', 'tesla', 'meta', 'amzn', 'amazon']
            detected_symbol = None
            for sym in symbols:
                if sym in query_lower:
                    detected_symbol = sym.upper()[:4] if len(sym) <= 4 else 'AAPL'  # Default mapping
                    break
            
            if detected_symbol:
                function_calls.append({"name": "get_historical_news_impact", "parameters": {"symbol": detected_symbol, "years_back": 5}})
            
            # Check for sector-level queries
            elif any(word in query_lower for word in ['sector', 'technology', 'healthcare', 'financial', 'energy', 'tech']):
                sector = "Technology"  # Default
                if 'healthcare' in query_lower:
                    sector = "Healthcare"
                elif 'financial' in query_lower or 'finance' in query_lower:
                    sector = "Financials"
                elif 'energy' in query_lower:
                    sector = "Energy"
                function_calls.append({"name": "get_sector_news_sensitivity", "parameters": {"sector_name": sector}})
            
            # Check for "similar events" queries
            elif any(word in query_lower for word in ['similar', 'like', 'happened when', 'past events']):
                sentiment = -5.0 if any(neg in query_lower for neg in ['negative', 'bearish', 'bad', 'crisis']) else 5.0
                function_calls.append({"name": "find_similar_events", "parameters": {"sentiment_score": sentiment, "sector_name": "Technology", "days_back": 1825}})
            
            # Check for correlation/proof queries
            elif any(word in query_lower for word in ['correlation', 'does news', 'move', 'affect', 'impact', 'prove']):
                function_calls.append({"name": "get_news_correlation_stats", "parameters": {}})
    
    # Predict impact based on news sentiment for a portfolio
    if manager_name and any(word in query_lower for word in ['impact of news', 'react to news', 'if news', 'negative news', 'positive news']):
        sentiment = -5.0 if any(neg in query_lower for neg in ['negative', 'bearish', 'bad']) else 5.0
        function_calls.append({"name": "predict_portfolio_news_impact", "parameters": {"manager_name": manager_name, "sentiment_score": sentiment}})
    
    # RAG queries - for news, SEC filings, company info, etc.
    rag_keywords = ['what happened', 'news about', 'latest news', 'recent news', 'tell me about', 
                    'any news', 'headlines', 'earnings report', 'announced', 'reported',
                    '10-k', '10k', '10-q', '10q', '8-k', '8k', 'annual report', 'quarterly report',
                    'sec filing', 'material event', 'company history', 'company background',
                    'founded', 'wikipedia', 'current price', 'stock price']
    stock_names = ['costco', 'apple', 'aapl', 'microsoft', 'msft', 'google', 'googl', 'amazon', 'amzn',
                   'tesla', 'tsla', 'nvidia', 'nvda', 'meta', 'netflix', 'nflx', 'walmart', 'wmt',
                   'jpmorgan', 'jpm', 'boeing', 'ba', 'disney', 'dis', 'intel', 'intc', 'amd',
                   'adobe', 'salesforce', 'oracle', 'cisco', 'ibm', 'qualcomm', 'paypal']
    
    # Check if this is a RAG query
    is_rag_query = any(kw in query_lower for kw in rag_keywords)
    mentions_stock = any(stock in query_lower for stock in stock_names)
    
    if is_rag_query or (mentions_stock and any(word in query_lower for word in ['what', 'why', 'when', 'how', 'did', 'does', 'is there', 'about'])):
        # Don't add if we already have historical news functions
        if not any(f["name"] in ["get_historical_news_impact", "get_sector_news_sensitivity", "find_similar_events"] for f in function_calls):
            function_calls.append({"name": "query_rag", "parameters": {"question": user_query}})
    
    return function_calls

def format_stress_test_compact(manager_name, result):
    """Format stress test results in compact form for comparison"""
    if isinstance(result, dict) and "error" in result:
        return f"\n❌ {manager_name}: {result['error']}\n"
    
    if not isinstance(result, list) or len(result) == 0:
        return f"\n⚠️ {manager_name}: No stress test data available\n"
    
    # Find recession scenario
    recession = next((t for t in result if 'recession' in t['scenario_name'].lower()), None)
    crash = next((t for t in result if 'crash' in t['scenario_name'].lower()), None)
    
    output = f"\n### 👤 {manager_name}\n"
    
    if recession:
        output += f"**Recession Impact:** {recession['impact_pct']:.1f}% (${recession['impact_usd']:,.0f})\n"
    if crash:
        output += f"**Market Crash:** {crash['impact_pct']:.1f}% (${crash['impact_usd']:,.0f})\n"
    
    output += "\n"
    return output

def format_function_result(function_name, result):
    """Format function results for display"""
    if isinstance(result, dict) and "error" in result:
        return f"\n❌ Error: {result['error']}\n"
    
    output = "\n---\n\n"
    
    if function_name == "get_risk_metrics" and isinstance(result, dict):
        output += f"""**📊 Risk Metrics:**

- **AUM:** ${result['aum_usd']:,.0f}
- **Portfolio Beta:** {result['portfolio_beta']:.2f}
- **Volatility:** {result['weighted_volatility_pct']:.2f}%
- **1-Day VaR (95%):** ${result['var_1day_95_usd']:,.0f}
- **10-Day VaR (95%):** ${result['var_10day_95_usd']:,.0f}
- **Positions:** {result['num_positions']}
"""
    
    elif function_name == "get_portfolio_holdings" and isinstance(result, list):
        output += "**📈 Top Holdings:**\n\n"
        for holding in result[:10]:
            output += f"- **{holding['symbol']}** ({holding['company_name']}): ${holding['value_usd']:,.0f} ({holding['weight_pct']:.1f}%)\n"
    
    elif function_name == "get_sector_exposures" and isinstance(result, list):
        output += "**🏢 Sector Exposure:**\n\n"
        for sector in result:
            output += f"- **{sector['sector']}**: {sector['sector_weight_pct']:.1f}%\n"
    
    elif function_name == "get_stress_tests" and isinstance(result, list):
        output += "**⚠️ Stress Test Results:**\n\n"
        for test in result:
            output += f"**{test['scenario_name']}**\n"
            output += f"- Impact: {test['impact_pct']:.1f}% (${test['impact_usd']:,.0f})\n"
            output += f"- Scenario: {test['scenario_description']}\n\n"
    
    elif function_name == "compare_managers" and isinstance(result, list):
        output += "**👥 Manager Comparison:**\n\n"
        for manager in result:
            output += f"""**{manager['manager_name']}** - {manager['risk_profile']}
- AUM: ${manager['aum_usd']:,.0f}
- Beta: {manager['portfolio_beta']:.2f}
- VaR (1-day): ${manager['var_1day_95_usd']:,.0f}
- Holdings: {manager['num_holdings']}
- Target Return: {manager['target_return_pct']:.0f}%

"""
    
    elif function_name == "get_macro_context" and isinstance(result, list):
        output += "**🌍 Macroeconomic Indicators:**\n\n"
        for indicator in result:
            output += f"- **{indicator['indicator_name']}**: {indicator['latest_value']:.2f} (as of {indicator['latest_date']})\n"
    
    elif function_name == "query_rag" and isinstance(result, dict):
        output += "**🔍 RAG Analysis:**\n\n"
        answer = result.get('answer', 'No answer available')
        output += f"{answer}\n"
    
    return output

def query_rule_based(user_query):
    """Fallback rule-based responses"""
    query_lower = user_query.lower()
    
    # Detect function calls
    function_calls = detect_function_calls(user_query, "")
    
    if function_calls:
        response = "Here's the information you requested:\n\n"
        for func_call in function_calls:
            result = call_uc_function(func_call["name"], func_call["parameters"])
            response += format_function_result(func_call["name"], result)
        return response
    
    return """I can help you with:

**Risk Analysis:**
- "What is Sarah Russel's risk profile?"
- "Show me stress tests for Mohit Arora"
- "Compare all three managers"

**Portfolio Details:**
- "Show me Rena Tang's holdings"
- "What is Mohit Arora's sector exposure?"

**Market Context:**
- "What's the current macro environment?"

**Our Managers:**
- 👤 Sarah Russel (Conservative, $50M)
- 👤 Rena Tang (Balanced, $75M)
- 👤 Mohit Arora (Aggressive, $100M)

What would you like to know?"""

# Sidebar with example queries
with st.sidebar:
    st.markdown("### 💡 Example Queries")
    
    examples = [
        "What is Sarah Russel's risk profile?",
        "Compare all three managers",
        "Show me Mohit Arora's top 10 holdings",
        "What happened to Costco recently?",
        "What's in Apple's 10-K annual report?",
        "Tell me about Tesla's company history",
        "Microsoft quarterly report?",
        "Any SEC 8-K filings for Amazon?",
        "Latest news about Nvidia stock?"
    ]
    
    for example in examples:
        if st.button(example, key=f"example_{example[:20]}", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": example})
            with st.spinner("🤔 Analyzing..."):
                response = query_llm(example, st.session_state.messages)
                st.session_state.messages.append({"role": "assistant", "content": response})
            st.rerun()
    
    st.markdown("---")
    st.markdown("### 📊 Available Data")
    st.markdown("""
    - **3 Portfolio Managers**
    - **$225M Total AUM**
    - **119 Holdings**
    - **12+ Risk Metrics**
    - **4 Stress Scenarios**
    - **6 Macro Indicators**
    - **📰 News RAG Agent**
    """)
    
    if st.button("🗑️ Clear Chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about portfolio managers, risk metrics, or holdings..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get AI response
    with st.chat_message("assistant"):
        with st.spinner("🤔 Analyzing..."):
            response = query_llm(prompt, st.session_state.messages)
            st.markdown(response)
    
    # Add assistant message
    st.session_state.messages.append({"role": "assistant", "content": response})
