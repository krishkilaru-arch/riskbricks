"""
Risk Dashboard
Visualize portfolio risk metrics, stress tests, and sector exposures
"""

import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

st.set_page_config(page_title="Risk Dashboard", page_icon="📊", layout="wide")

# Page header
st.title("📊 Risk Dashboard")
st.markdown("Comprehensive risk analytics and visualizations for all portfolio managers.")

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

@st.cache_data(ttl=300)
def get_risk_metrics():
    """Fetch portfolio risk metrics"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            manager_name,
            risk_profile,
            total_value_usd,
            portfolio_beta,
            weighted_volatility,
            var_1day_95,
            var_10day_95,
            num_positions
        FROM riskbricks.gold.portfolio_risk_metrics
        ORDER BY total_value_usd DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching risk metrics: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_stress_tests():
    """Fetch stress test results"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            manager_name,
            scenario_name,
            scenario_description,
            total_impact_usd,
            impact_percentage
        FROM riskbricks.gold.stress_test_results
        ORDER BY ABS(impact_percentage) DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching stress tests: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_sector_exposures():
    """Fetch sector exposures"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            manager_name,
            sector,
            sector_weight
        FROM riskbricks.gold.sector_exposures
        ORDER BY manager_name, sector_weight DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching sector exposures: {str(e)}")
        return pd.DataFrame()

# Fetch data
risk_metrics_df = get_risk_metrics()
stress_tests_df = get_stress_tests()
sector_exposures_df = get_sector_exposures()

if risk_metrics_df.empty:
    st.warning("No risk metrics found. Please run the analytics pipeline first.")
    if st.button("📖 View Setup Guide"):
        st.markdown("""
        ### Setup Steps:
        1. Run `00_setup_multi_manager_portfolios.py` to create managers
        2. Run `01_data_ingestion.py` to ingest market data
        3. Run `02_data_validation.py` to validate data quality
        4. Run `03_risk_analytics.py` to compute risk metrics
        """)
    st.stop()

# Sidebar filters
with st.sidebar:
    st.markdown("### 🎛️ Filters")
    
    selected_managers = st.multiselect(
        "Select Managers",
        risk_metrics_df['manager_name'].unique().tolist(),
        default=risk_metrics_df['manager_name'].unique().tolist()
    )
    
    st.markdown("---")
    
    metric_view = st.radio(
        "Metric View",
        ["Absolute Values", "As % of AUM"]
    )
    
    st.markdown("---")
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Filter data
filtered_risk = risk_metrics_df[risk_metrics_df['manager_name'].isin(selected_managers)]
filtered_stress = stress_tests_df[stress_tests_df['manager_name'].isin(selected_managers)]
filtered_sector = sector_exposures_df[sector_exposures_df['manager_name'].isin(selected_managers)]

# Overview metrics
st.markdown("## 📈 Portfolio Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_aum = filtered_risk['total_value_usd'].sum()
    st.metric("Total AUM", f"${total_aum/1e6:.1f}M")

with col2:
    avg_beta = (filtered_risk['portfolio_beta'] * filtered_risk['total_value_usd']).sum() / total_aum
    st.metric("Weighted Avg Beta", f"{avg_beta:.2f}")

with col3:
    total_var_1day = filtered_risk['var_1day_95'].sum()
    st.metric("Total 1-Day VaR", f"${total_var_1day/1e6:.2f}M")

with col4:
    var_pct = (total_var_1day / total_aum) * 100
    st.metric("VaR as % of AUM", f"{var_pct:.2f}%")

st.markdown("---")

# Portfolio Manager Insights
st.markdown("## 🧭 Portfolio Manager Insights")

@st.cache_data(ttl=300)
def get_accuracy_scoreboard():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = """
        SELECT symbol, horizon_days, window_start, window_end, hit_rate, mae, rmse, mape, sample_size
        FROM riskbricks.gold.accuracy_scoreboard_daily
        ORDER BY window_end DESC, hit_rate DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching accuracy scoreboard: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_decision_signals():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = """
        SELECT symbol, forecast_date, horizon_days, decision, expected_return,
               predicted_price, last_close, confidence_band_low, confidence_band_high
        FROM riskbricks.gold.decision_signals
        ORDER BY forecast_date DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching decision signals: {str(e)}")
        return pd.DataFrame()

scoreboard_df = get_accuracy_scoreboard()
signals_df = get_decision_signals()

if scoreboard_df.empty:
    st.warning("No accuracy scoreboard found. Run build_portfolio_manager_outputs.")
else:
    st.dataframe(scoreboard_df, use_container_width=True)

if signals_df.empty:
    st.warning("No decision signals found. Run build_portfolio_manager_outputs.")
else:
    st.dataframe(signals_df, use_container_width=True)

# Risk Metrics Section
st.markdown("## 📊 Risk Metrics Comparison")

tab1, tab2, tab3 = st.tabs(["VaR Analysis", "Beta & Volatility", "Portfolio Composition"])

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        # 1-Day VaR comparison
        if metric_view == "As % of AUM":
            filtered_risk['var_1day_pct'] = (filtered_risk['var_1day_95'] / filtered_risk['total_value_usd']) * 100
            fig = px.bar(
                filtered_risk,
                x='manager_name',
                y='var_1day_pct',
                title='1-Day VaR (95% Confidence) as % of AUM',
                labels={'var_1day_pct': 'VaR (%)'},
                color='risk_profile',
                color_discrete_map={'Conservative': '#4CAF50', 'Balanced': '#FFC107', 'Aggressive': '#F44336'}
            )
        else:
            fig = px.bar(
                filtered_risk,
                x='manager_name',
                y='var_1day_95',
                title='1-Day VaR (95% Confidence)',
                labels={'var_1day_95': 'VaR (USD)'},
                color='risk_profile',
                color_discrete_map={'Conservative': '#4CAF50', 'Balanced': '#FFC107', 'Aggressive': '#F44336'}
            )
        fig.update_layout(showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 10-Day VaR comparison
        if metric_view == "As % of AUM":
            filtered_risk['var_10day_pct'] = (filtered_risk['var_10day_95'] / filtered_risk['total_value_usd']) * 100
            fig = px.bar(
                filtered_risk,
                x='manager_name',
                y='var_10day_pct',
                title='10-Day VaR (95% Confidence) as % of AUM',
                labels={'var_10day_pct': 'VaR (%)'},
                color='risk_profile',
                color_discrete_map={'Conservative': '#4CAF50', 'Balanced': '#FFC107', 'Aggressive': '#F44336'}
            )
        else:
            fig = px.bar(
                filtered_risk,
                x='manager_name',
                y='var_10day_95',
                title='10-Day VaR (95% Confidence)',
                labels={'var_10day_95': 'VaR (USD)'},
                color='risk_profile',
                color_discrete_map={'Conservative': '#4CAF50', 'Balanced': '#FFC107', 'Aggressive': '#F44336'}
            )
        fig.update_layout(showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    # VaR table
    st.markdown("### Detailed VaR Metrics")
    display_df = filtered_risk[['manager_name', 'risk_profile', 'total_value_usd', 'var_1day_95', 'var_10day_95']].copy()
    display_df['var_1day_pct'] = (display_df['var_1day_95'] / display_df['total_value_usd']) * 100
    display_df['var_10day_pct'] = (display_df['var_10day_95'] / display_df['total_value_usd']) * 100
    
    st.dataframe(
        display_df.style.format({
            'total_value_usd': '${:,.0f}',
            'var_1day_95': '${:,.0f}',
            'var_10day_95': '${:,.0f}',
            'var_1day_pct': '{:.2f}%',
            'var_10day_pct': '{:.2f}%'
        }),
        use_container_width=True
    )

with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        # Portfolio Beta
        fig = px.bar(
            filtered_risk,
            x='manager_name',
            y='portfolio_beta',
            title='Portfolio Beta (Market Sensitivity)',
            labels={'portfolio_beta': 'Beta'},
            color='portfolio_beta',
            color_continuous_scale='RdYlGn_r'
        )
        fig.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="Market Beta = 1.0")
        st.plotly_chart(fig, use_container_width=True)
        
        st.info("""
        **Beta Interpretation:**
        - Beta < 0.8: Less volatile than market
        - Beta 0.8-1.2: Similar to market
        - Beta > 1.2: More volatile than market
        """)
    
    with col2:
        # Volatility
        filtered_risk['volatility_pct'] = filtered_risk['weighted_volatility'] * 100
        fig = px.bar(
            filtered_risk,
            x='manager_name',
            y='volatility_pct',
            title='Weighted Portfolio Volatility',
            labels={'volatility_pct': 'Volatility (%)'},
            color='volatility_pct',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Scatter: Beta vs Volatility
    st.markdown("### Risk-Return Profile")
    fig = px.scatter(
        filtered_risk,
        x='portfolio_beta',
        y='volatility_pct',
        size='total_value_usd',
        color='risk_profile',
        hover_data=['manager_name', 'num_positions'],
        title='Portfolio Beta vs Volatility',
        labels={'portfolio_beta': 'Portfolio Beta', 'volatility_pct': 'Volatility (%)'},
        color_discrete_map={'Conservative': '#4CAF50', 'Balanced': '#FFC107', 'Aggressive': '#F44336'}
    )
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        # Portfolio size comparison
        fig = px.pie(
            filtered_risk,
            values='total_value_usd',
            names='manager_name',
            title='AUM Distribution'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Number of positions
        fig = px.bar(
            filtered_risk,
            x='manager_name',
            y='num_positions',
            title='Number of Positions',
            labels={'num_positions': 'Positions'},
            color='num_positions',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)

# Stress Tests Section
st.markdown("---")
st.markdown("## 🎯 Stress Test Results")

if not filtered_stress.empty:
    # Stress test heatmap
    stress_pivot = filtered_stress.pivot(
        index='scenario_name',
        columns='manager_name',
        values='impact_percentage'
    )
    
    fig = go.Figure(data=go.Heatmap(
        z=stress_pivot.values,
        x=stress_pivot.columns,
        y=stress_pivot.index,
        colorscale='RdYlGn_r',
        text=stress_pivot.values,
        texttemplate='%{text:.1f}%',
        textfont={"size": 12},
        colorbar=dict(title="Impact %")
    ))
    fig.update_layout(
        title='Stress Test Impact (% of Portfolio)',
        xaxis_title='Portfolio Manager',
        yaxis_title='Scenario',
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Scenario comparison
    st.markdown("### Scenario Details")
    scenario = st.selectbox(
        "Select Scenario",
        filtered_stress['scenario_name'].unique().tolist()
    )
    
    scenario_data = filtered_stress[filtered_stress['scenario_name'] == scenario]
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig = px.bar(
            scenario_data,
            x='manager_name',
            y='total_impact_usd',
            title=f'{scenario} - Dollar Impact',
            labels={'total_impact_usd': 'Impact (USD)'},
            color='manager_name'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown(f"**{scenario}**")
        st.markdown(f"_{scenario_data.iloc[0]['scenario_description']}_")
        
        st.markdown("**Impact by Manager:**")
        for _, row in scenario_data.iterrows():
            st.metric(
                row['manager_name'],
                f"{row['impact_percentage']:.1f}%",
                f"${row['total_impact_usd']/1e6:.2f}M"
            )
else:
    st.info("No stress test results available.")

# Sector Exposure Section
st.markdown("---")
st.markdown("## 🏢 Sector Exposure Analysis")

if not filtered_sector.empty:
    # Sector exposure by manager (stacked bar)
    fig = px.bar(
        filtered_sector,
        x='manager_name',
        y='sector_weight',
        color='sector',
        title='Sector Allocation by Manager',
        labels={'sector_weight': 'Weight'},
        barmode='stack'
    )
    fig.update_layout(yaxis_tickformat=',.0%')
    st.plotly_chart(fig, use_container_width=True)
    
    # Individual manager sector breakdown
    st.markdown("### Manager Sector Breakdown")
    
    selected_manager_sector = st.selectbox(
        "Select Manager for Detailed View",
        filtered_sector['manager_name'].unique().tolist()
    )
    
    manager_sectors = filtered_sector[filtered_sector['manager_name'] == selected_manager_sector]
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            manager_sectors,
            values='sector_weight',
            names='sector',
            title=f'{selected_manager_sector} - Sector Allocation'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.dataframe(
            manager_sectors[['sector', 'sector_weight']].sort_values('sector_weight', ascending=False).style.format({
                'sector_weight': '{:.2%}'
            }),
            use_container_width=True
        )
else:
    st.info("No sector exposure data available.")

# Footer
st.markdown("---")
st.markdown("""
### 📖 Dashboard Guide

**Value at Risk (VaR):**
- Potential loss at 95% confidence over 1 day or 10 days
- Higher VaR = Higher risk

**Beta:**
- Measures market sensitivity
- Beta > 1: More volatile than market
- Beta < 1: Less volatile than market

**Stress Tests:**
- Simulate extreme market scenarios
- Show potential portfolio impacts
- 4 scenarios: Market Crash, Tech Drawdown, Rate Spike, Recession

**Sector Exposure:**
- Diversification across industries
- High concentration = Higher sector risk
""")
