"""
Portfolio Management Interface
Add, edit, and manage portfolio managers and their holdings
"""

import streamlit as st
from databricks import sql
import pandas as pd
import os
from datetime import datetime
import uuid

st.set_page_config(page_title="Portfolio Management", page_icon="👥", layout="wide")

# Page header
st.title("👥 Portfolio Management")
st.markdown("Add new portfolio managers, create portfolios, and manage holdings.")

# Database connection
@st.cache_resource
def get_db_connection():
    """Get Databricks SQL connection"""
    try:
        token = os.getenv('DATABRICKS_TOKEN')
        hostname = os.getenv('DATABRICKS_HOST')
        warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID', 'default')
        
        if not token or not hostname:
            st.error("Missing DATABRICKS_TOKEN or DATABRICKS_HOST environment variables")
            return None
        
        return sql.connect(
            server_hostname=hostname,
            http_path=f'/sql/1.0/warehouses/{warehouse_id}',
            access_token=token
        )
    except Exception as e:
        st.error(f"Failed to connect to Databricks: {str(e)}")
        return None

def get_existing_managers():
    """Fetch existing portfolio managers"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            m.manager_id,
            m.manager_name,
            m.risk_profile,
            m.strategy_description,
            COALESCE(SUM(h.value_usd), 0) as total_value,
            m.target_return_pct,
            m.max_volatility_pct,
            m.beta_min,
            m.beta_max,
            m.created_date
        FROM riskbricks.gold.portfolio_managers m
        LEFT JOIN riskbricks.gold.portfolio_holdings h ON m.manager_id = h.manager_id
        GROUP BY m.manager_id, m.manager_name, m.risk_profile, m.strategy_description,
                 m.target_return_pct, m.max_volatility_pct, m.beta_min, m.beta_max, m.created_date
        ORDER BY m.manager_name
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching managers: {str(e)}")
        return pd.DataFrame()

def get_manager_holdings(manager_id):
    """Fetch holdings for a specific manager"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = f"""
        SELECT
            h.symbol,
            c.company_name,
            c.sector,
            h.value_usd,
            h.weight,
            c.beta,
            c.volatility_30d
        FROM riskbricks.gold.portfolio_holdings h
        JOIN riskbricks.gold.company_universe c ON h.symbol = c.symbol
        WHERE h.manager_id = '{manager_id}'
        ORDER BY h.value_usd DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching holdings: {str(e)}")
        return pd.DataFrame()

def get_available_stocks():
    """Get list of available stocks"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            symbol,
            company_name,
            sector,
            beta,
            volatility_30d
        FROM riskbricks.gold.company_universe
        ORDER BY symbol
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching stocks: {str(e)}")
        return pd.DataFrame()

def add_new_manager(manager_data):
    """Add a new portfolio manager"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO riskbricks.gold.portfolio_managers
            (manager_id, manager_name, risk_profile, strategy_description, 
             target_return_pct, max_volatility_pct, beta_min, beta_max, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            # Calculate beta range based on risk profile
            risk_profile = manager_data['risk_profile']
            if risk_profile == 'Conservative':
                beta_min, beta_max = 0.6, 0.9
            elif risk_profile == 'Balanced':
                beta_min, beta_max = 0.9, 1.1
            else:  # Aggressive
                beta_min, beta_max = 1.2, 1.8
            
            cursor.execute(query, (
                manager_data['manager_id'],
                manager_data['manager_name'],
                manager_data['risk_profile'],
                manager_data['strategy_description'],
                manager_data['target_return_pct'],
                manager_data['max_volatility_pct'],
                beta_min,
                beta_max,
                datetime.now().date()
            ))
        return True
    except Exception as e:
        st.error(f"Error adding manager: {str(e)}")
        return False

def add_holding(manager_id, symbol, value_usd, weight):
    """Add a holding to a portfolio"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO riskbricks.gold.portfolio_holdings
            (manager_id, symbol, sector, value_usd, weight, purchase_date, as_of_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            # Get sector from company_universe
            cursor2 = conn.cursor()
            cursor2.execute(f"SELECT sector FROM riskbricks.gold.company_universe WHERE symbol = '{symbol}'")
            sector_result = cursor2.fetchone()
            sector = sector_result[0] if sector_result else 'Unknown'
            cursor2.close()
            
            # Convert numpy types to Python native types
            value_usd_py = float(value_usd) if hasattr(value_usd, 'item') else value_usd
            weight_py = float(weight) if hasattr(weight, 'item') else weight
            
            cursor.execute(query, (
                manager_id, 
                symbol, 
                sector,
                value_usd_py, 
                weight_py, 
                datetime.now().date(),
                datetime.now().date()
            ))
        return True
    except Exception as e:
        st.error(f"Error adding holding: {str(e)}")
        return False

# Main tabs
tab1, tab2, tab3 = st.tabs(["📋 View Managers", "➕ Add New Manager", "📊 Manage Holdings"])

with tab1:
    st.markdown("### Current Portfolio Managers")
    
    managers_df = get_existing_managers()
    
    if not managers_df.empty:
        # Display managers
        for idx, manager in managers_df.iterrows():
            with st.expander(f"👤 {manager['manager_name']} - {manager['risk_profile']}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("AUM", f"${manager['total_value']/1e6:.1f}M")
                    st.metric("Risk Profile", manager['risk_profile'])
                
                with col2:
                    st.metric("Target Return", f"{manager['target_return_pct']:.1f}%")
                    st.metric("Max Volatility", f"{manager['max_volatility_pct']:.1f}%")
                
                with col3:
                    st.markdown(f"**Strategy:** {manager['strategy_description']}")
                    st.markdown(f"**Created:** {manager['created_date']}")
                
                # Show holdings
                st.markdown("#### Holdings")
                holdings_df = get_manager_holdings(manager['manager_id'])
                if not holdings_df.empty:
                    st.dataframe(
                        holdings_df.style.format({
                            'value_usd': '${:,.0f}',
                            'weight': '{:.2%}',
                            'beta': '{:.2f}',
                            'volatility_30d': '{:.2%}'
                        }),
                        use_container_width=True
                    )
                    
                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Positions", len(holdings_df))
                    with col2:
                        st.metric("Weighted Beta", f"{(holdings_df['weight'] * holdings_df['beta']).sum():.2f}")
                    with col3:
                        st.metric("Weighted Vol", f"{(holdings_df['weight'] * holdings_df['volatility_30d']).sum():.2%}")
                else:
                    st.info("No holdings yet. Add holdings in the 'Manage Holdings' tab.")
    else:
        st.info("No portfolio managers found. Add your first manager in the 'Add New Manager' tab.")

with tab2:
    st.markdown("### Add New Portfolio Manager")
    
    with st.form("add_manager_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            manager_name = st.text_input("Manager Name*", placeholder="e.g., John Smith")
            risk_profile = st.selectbox(
                "Risk Profile*",
                ["Conservative", "Balanced", "Aggressive", "Custom"]
            )
            total_aum = st.number_input(
                "Initial AUM (USD)*",
                min_value=1000000.0,
                max_value=1000000000.0,
                value=50000000.0,
                step=1000000.0,
                format="%.0f"
            )
        
        with col2:
            target_return = st.number_input(
                "Target Return (%)*",
                min_value=0.0,
                max_value=50.0,
                value=10.0,
                step=0.5,
                format="%.1f"
            )
            max_volatility = st.number_input(
                "Max Volatility (%)*",
                min_value=0.0,
                max_value=100.0,
                value=15.0,
                step=1.0,
                format="%.1f"
            )
        
        strategy_description = st.text_area(
            "Investment Strategy*",
            placeholder="e.g., Focus on blue-chip stocks with strong dividends and low volatility",
            height=100
        )
        
        submitted = st.form_submit_button("➕ Create Manager", type="primary")
        
        if submitted:
            if not manager_name or not strategy_description:
                st.error("Please fill in all required fields (*)!")
            else:
                manager_data = {
                    'manager_id': str(uuid.uuid4()),
                    'manager_name': manager_name,
                    'risk_profile': risk_profile,
                    'strategy_description': strategy_description,
                    'target_return_pct': target_return,
                    'max_volatility_pct': max_volatility
                }
                
                if add_new_manager(manager_data):
                    st.success(f"✅ Successfully created portfolio manager: {manager_name}")
                    st.balloons()
                    st.info("Next step: Add holdings to this portfolio in the 'Manage Holdings' tab.")
                    st.rerun()
                else:
                    st.error("Failed to create manager. Please try again.")
    
    st.markdown("---")
    st.markdown("""
    ### 💡 Risk Profile Guidelines
    
    - **Conservative:** Target return 5-8%, Max volatility 10-12%, Beta range 0.6-0.9
    - **Balanced:** Target return 9-12%, Max volatility 13-16%, Beta range 0.9-1.1
    - **Aggressive:** Target return 15-20%, Max volatility 20-30%, Beta range 1.2-1.8
    - **Custom:** Define your own parameters
    """)

with tab3:
    st.markdown("### Manage Portfolio Holdings")
    
    # Select manager
    managers_df = get_existing_managers()
    if managers_df.empty:
        st.warning("No portfolio managers found. Please add a manager first.")
    else:
        selected_manager_name = st.selectbox(
            "Select Portfolio Manager",
            managers_df['manager_name'].tolist()
        )
        
        selected_manager = managers_df[managers_df['manager_name'] == selected_manager_name].iloc[0]
        manager_id = selected_manager['manager_id']
        
        # Show current holdings
        st.markdown("#### Current Holdings")
        holdings_df = get_manager_holdings(manager_id)
        if not holdings_df.empty:
            st.dataframe(holdings_df, use_container_width=True)
        else:
            st.info("No holdings yet. Add your first position below.")
        
        st.markdown("---")
        st.markdown("#### Add New Holding")
        
        # Get available stocks
        stocks_df = get_available_stocks()
        
        if not stocks_df.empty:
            with st.form("add_holding_form"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    selected_stock = st.selectbox(
                        "Select Stock*",
                        stocks_df['symbol'].tolist(),
                        format_func=lambda x: f"{x} - {stocks_df[stocks_df['symbol']==x]['company_name'].iloc[0]}"
                    )
                    stock_info = stocks_df[stocks_df['symbol'] == selected_stock].iloc[0]
                    st.info(f"**Sector:** {stock_info['sector']}\n**Beta:** {stock_info['beta']:.2f}\n**Vol:** {stock_info['volatility_30d']:.2%}")
                
                with col2:
                    value_usd = st.number_input(
                        "Value (USD)*",
                        min_value=1000.0,
                        max_value=float(selected_manager['total_value']),
                        value=1000000.0,
                        step=10000.0,
                        format="%.0f"
                    )
                
                with col3:
                    weight = value_usd / selected_manager['total_value']
                    st.metric("Portfolio Weight", f"{weight:.2%}")
                    
                    # Calculate remaining capacity
                    if not holdings_df.empty:
                        current_total = holdings_df['value_usd'].sum()
                        remaining = selected_manager['total_value'] - current_total
                        st.metric("Remaining Capacity", f"${remaining/1e6:.1f}M")
                
                add_holding_button = st.form_submit_button("➕ Add Holding", type="primary")
                
                if add_holding_button:
                    if add_holding(manager_id, selected_stock, value_usd, weight):
                        st.success(f"✅ Successfully added {selected_stock} (${value_usd:,.0f})")
                        st.rerun()
                    else:
                        st.error("Failed to add holding. Please try again.")
        else:
            st.error("No stocks available. Please run the data ingestion pipeline first.")
        
        st.markdown("---")
        st.markdown("### ⚙️ Run Analytics")
        st.markdown("After adding or modifying holdings, run the analytics pipeline to compute risk metrics.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Run Risk Analytics", type="primary"):
                st.info("This will trigger notebook: `03_risk_analytics.py`")
                st.markdown("**Note:** In production, this button would trigger a Databricks job to recompute all risk metrics.")
        
        with col2:
            if st.button("📊 View Risk Dashboard"):
                st.switch_page("pages/3_📊_Risk_Dashboard.py")

# Footer
st.markdown("---")
st.markdown("""
### 📖 Next Steps

1. **Add a new manager** in the "Add New Manager" tab
2. **Add holdings** to the portfolio in the "Manage Holdings" tab
3. **Run analytics** to compute VaR, beta, and stress tests
4. **View results** in the Risk Dashboard
5. **Query the AI** to get insights about the new portfolio

The AI agent will automatically have access to the new manager and can answer questions about it.
""")
