"""
RiskBricks - AI-Powered Portfolio Risk Analytics
Main application entry point
"""

import streamlit as st
from databricks import sql
import os

# Page config
st.set_page_config(
    page_title="RiskBricks",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #FF3621;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #FF3621;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'databricks_connection' not in st.session_state:
    st.session_state.databricks_connection = None

def get_db_connection():
    """Get Databricks SQL connection"""
    if st.session_state.databricks_connection is None:
        # When running as Databricks App, credentials are auto-provided
        try:
            # Check if credentials are available
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
            http_path = os.getenv("DATABRICKS_HTTP_PATH")
            access_token = os.getenv("DATABRICKS_TOKEN")
            
            if not all([server_hostname, http_path, access_token]):
                st.warning("⚠️ Databricks credentials not yet available. Please refresh the page.")
                return None
            
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            )
            st.session_state.databricks_connection = connection
        except Exception as e:
            st.error(f"Failed to connect to Databricks: {str(e)}")
            return None
    return st.session_state.databricks_connection

def get_summary_stats():
    """Get summary statistics for the home page"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cursor:
            # Get portfolio manager count
            cursor.execute("SELECT COUNT(*) FROM riskbricks.gold.portfolio_managers")
            num_managers = cursor.fetchone()[0]
            
            # Get total AUM
            cursor.execute("SELECT SUM(total_value_usd) FROM riskbricks.gold.portfolio_managers")
            total_aum = cursor.fetchone()[0]
            
            # Get total positions
            cursor.execute("SELECT COUNT(*) FROM riskbricks.gold.portfolio_holdings")
            num_positions = cursor.fetchone()[0]
            
            # Get stock universe size
            cursor.execute("SELECT COUNT(*) FROM riskbricks.gold.company_universe")
            num_stocks = cursor.fetchone()[0]
            
            # Get data freshness
            cursor.execute("SELECT MAX(date) FROM riskbricks.silver.stock_prices")
            latest_data = cursor.fetchone()[0]
            
            return {
                'num_managers': num_managers,
                'total_aum': total_aum,
                'num_positions': num_positions,
                'num_stocks': num_stocks,
                'latest_data': latest_data
            }
    except Exception as e:
        st.error(f"Error fetching stats: {str(e)}")
        return None

# Main content
st.markdown('<div class="main-header">📊 RiskBricks</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">AI-Powered Portfolio Risk Analytics Platform</div>', unsafe_allow_html=True)

# Introduction
st.markdown("""
Welcome to **RiskBricks** - the next-generation portfolio risk management system powered by 
multi-agent AI and real-time market data.
""")

# Get summary stats - wrapped in try/except to prevent crashes
stats = None
try:
    conn = get_db_connection()
    if conn is not None:
        stats = get_summary_stats()
except Exception as e:
    st.warning(f"⚠️ Could not load live data: {str(e)}")
    st.info("💡 The app is running, but data connection is not available yet. This is normal on first load.")

if stats:
    # Display key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Portfolio Managers", stats['num_managers'])
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Total AUM", f"${stats['total_aum']/1e6:.1f}M")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Total Positions", stats['num_positions'])
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Stock Universe", stats['num_stocks'])
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Data freshness
    st.markdown(f"""
    <div class="info-box">
    <strong>📅 Data Freshness:</strong> Latest market data as of <strong>{stats['latest_data']}</strong>
    </div>
    """, unsafe_allow_html=True)
else:
    # Show placeholder metrics
    st.info("📊 RiskBricks is initializing... Data will load momentarily. Try refreshing the page in 10 seconds.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Portfolio Managers", "Loading...")
    with col2:
        st.metric("Total AUM", "Loading...")
    with col3:
        st.metric("Total Positions", "Loading...")
    with col4:
        st.metric("Stock Universe", "Loading...")

# Features overview
st.markdown("## 🚀 Platform Features")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    ### 🤖 AI Agent Chat
    - Natural language queries
    - Multi-agent orchestration
    - 300+ query types supported
    - Intelligent risk insights
    
    ### 📊 Risk Dashboard
    - Real-time risk metrics
    - VaR calculations (1-day & 10-day)
    - Portfolio beta & volatility
    - Stress test scenarios
    """)

with col2:
    st.markdown("""
    ### 👥 Portfolio Management
    - Add new managers
    - Create portfolios
    - Edit holdings
    - Set risk constraints
    
    ### 📈 Analytics
    - Sector exposure analysis
    - Correlation matrices
    - Custom portfolio analysis
    - Historical performance
    """)

st.markdown("---")

# Quick start guide
st.markdown("## 🎯 Quick Start")

tab1, tab2, tab3, tab4 = st.tabs(["💬 Ask the AI", "👥 Manage Portfolios", "📊 View Dashboard", "🔧 Settings"])

with tab1:
    st.markdown("""
    ### Chat with the AI Agent
    
    1. Go to **🤖 AI Agent Chat** in the sidebar
    2. Type your question in natural language
    3. Get instant insights and analysis
    
    **Example queries:**
    - "Compare all three portfolio managers"
    - "What is Mohit's technology sector exposure?"
    - "Show me stress test results for a market crash"
    - "Find low-risk healthcare stocks"
    - "What's the correlation between Apple and Microsoft?"
    """)

with tab2:
    st.markdown("""
    ### Add a New Portfolio Manager
    
    1. Go to **👥 Portfolio Management** in the sidebar
    2. Click "Add New Manager"
    3. Fill in manager details:
       - Name and risk profile
       - Target return and volatility limits
       - Investment strategy
    4. Add holdings to the portfolio
    5. Run analytics to compute risk metrics
    
    **The system will automatically:**
    - Calculate VaR and beta
    - Run stress tests
    - Compute sector exposures
    - Update the AI agent
    """)

with tab3:
    st.markdown("""
    ### View Risk Analytics
    
    1. Go to **📊 Risk Dashboard** in the sidebar
    2. Select a portfolio manager
    3. View comprehensive risk metrics:
       - Value at Risk (VaR)
       - Portfolio beta
       - Sector exposures
       - Stress test results
    4. Compare across managers
    5. Export reports
    """)

with tab4:
    st.markdown("""
    ### System Configuration
    
    1. Go to **⚙️ Data Management** in the sidebar
    2. Configure data sources:
       - FRED API for macro indicators
       - Yahoo Finance for stock prices
    3. Schedule data refresh
    4. View ingestion logs
    5. Manage API keys (stored in Databricks Secrets)
    """)

st.markdown("---")

# System architecture
with st.expander("🏗️ System Architecture"):
    st.markdown("""
    ### Data Pipeline (Medallion Architecture)
    
    **Bronze Layer:**
    - Raw stock prices from Yahoo Finance
    - Macro indicators from FRED
    - 12 years of historical data
    
    **Silver Layer:**
    - Data validation and quality checks
    - Anomaly detection
    - Clean, curated data
    
    **Gold Layer:**
    - Portfolio risk metrics
    - Stress test results
    - Sector exposures
    - Business-ready analytics
    
    ### AI Agent System
    
    **Multi-Agent Supervisor:**
    - Deployed on Databricks Agent Bricks
    - 12 Unity Catalog Functions as tools
    - Llama 3.3 70B Instruct LLM
    - REST API endpoint
    
    **Capabilities:**
    - Risk metrics calculation
    - Stress testing
    - Portfolio analysis
    - Stock research
    - Correlation analysis
    - Custom portfolio evaluation
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666;">
    <p><strong>RiskBricks</strong> - Built with Databricks, Unity Catalog, and Agent Bricks</p>
    <p>Data sources: FRED (Federal Reserve Economic Data) | Yahoo Finance</p>
</div>
""", unsafe_allow_html=True)
