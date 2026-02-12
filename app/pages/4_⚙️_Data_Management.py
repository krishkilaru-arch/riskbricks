"""
Data Management Interface
Control data ingestion, view freshness, and manage pipelines
"""

import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import pandas as pd
import os
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="Data Management", page_icon="⚙️", layout="wide")

# Page header
st.title("⚙️ Data Management")
st.markdown("Monitor data freshness, trigger ingestion, and manage the data pipeline.")

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

@st.cache_resource
def get_workspace_client():
    """Get Databricks Workspace Client for Jobs API"""
    try:
        hostname = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        
        if not hostname or not token:
            return None
        
        # Ensure hostname has https://
        if not hostname.startswith('http'):
            hostname = f'https://{hostname}'
        
        return WorkspaceClient(host=hostname, token=token)
    except Exception as e:
        st.error(f"Failed to create Workspace Client: {str(e)}")
        return None

def find_job_by_name(job_name):
    """Find a job by name and return its ID"""
    w = get_workspace_client()
    if not w:
        return None
    
    try:
        # List all jobs and find by name
        job_list = w.jobs.list(name=job_name)
        for job in job_list:
            if job.settings and job.settings.name == job_name:
                return job.job_id
        return None
    except Exception as e:
        st.error(f"Error finding job '{job_name}': {str(e)}")
        return None

def trigger_job(job_name):
    """Trigger a Databricks job and return run info"""
    w = get_workspace_client()
    if not w:
        st.error("❌ Could not connect to Databricks workspace")
        return None
    
    # Find job by name
    job_id = find_job_by_name(job_name)
    
    if not job_id:
        st.warning(f"⚠️ Job '{job_name}' not found. Please create it first.")
        st.info(f"""
        **To create this job:**
        1. Go to Workflows → Jobs in Databricks
        2. Create a job named: `{job_name}`
        3. Or use the CLI: See docs/SETUP_JOB.md
        """)
        return None
    
    try:
        # Trigger the job
        run = w.jobs.run_now(job_id=job_id)
        return {
            'job_id': job_id,
            'run_id': run.run_id,
            'job_name': job_name
        }
    except Exception as e:
        st.error(f"❌ Error triggering job: {str(e)}")
        return None

def get_run_status(run_id):
    """Get the status of a job run"""
    w = get_workspace_client()
    if not w:
        return None
    
    try:
        run = w.jobs.get_run(run_id=run_id)
        return {
            'state': run.state.life_cycle_state.value if run.state else 'UNKNOWN',
            'result_state': run.state.result_state.value if run.state and run.state.result_state else None,
            'start_time': run.start_time,
            'end_time': run.end_time
        }
    except Exception as e:
        return {'state': 'ERROR', 'error': str(e)}

@st.cache_data(ttl=60)
def get_data_freshness():
    """Get data freshness metrics"""
    conn = get_db_connection()
    if not conn:
        st.error("Database connection failed")
        return {}
    
    try:
        metrics = {}
        
        with conn.cursor() as cursor:
            # Stock prices freshness
            try:
                cursor.execute("SELECT MAX(date) as latest_date, COUNT(DISTINCT symbol) as num_symbols, COUNT(*) as total_records FROM riskbricks.bronze.stock_prices_bronze")
                row = cursor.fetchone()
                metrics['stock_prices'] = {
                    'latest_date': row[0],
                    'num_symbols': row[1],
                    'total_records': row[2]
                }
            except Exception as e:
                st.warning(f"Could not fetch stock prices data: {str(e)[:100]}")
                metrics['stock_prices'] = {'latest_date': 'N/A', 'num_symbols': 0, 'total_records': 0}
            
            # Macro indicators freshness
            try:
                cursor.execute("SELECT MAX(date) as latest_date, COUNT(DISTINCT indicator_name) as num_indicators, COUNT(*) as total_records FROM riskbricks.bronze.macro_indicators_bronze")
                row = cursor.fetchone()
                metrics['macro_indicators'] = {
                    'latest_date': row[0],
                    'num_indicators': row[1],
                    'total_records': row[2]
                }
            except Exception as e:
                st.warning(f"Could not fetch macro indicators: {str(e)[:100]}")
                metrics['macro_indicators'] = {'latest_date': 'N/A', 'num_indicators': 0, 'total_records': 0}
            
            # Data quality (Silver layer)
            try:
                cursor.execute("""
                    SELECT 
                        'stock_prices' as data_type,
                        COUNT(*) as total_records,
                        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
                        AVG(quality_score) as avg_quality
                    FROM riskbricks.silver.stock_prices
                """)
                row = cursor.fetchone()
                metrics['stock_quality'] = {
                    'total_records': row[1],
                    'anomalies': row[2],
                    'avg_quality': row[3]
                }
            except Exception as e:
                st.warning(f"Could not fetch data quality metrics: {str(e)[:100]}")
                metrics['stock_quality'] = {'total_records': 0, 'anomalies': 0, 'avg_quality': 0}
            
            # Company universe
            try:
                cursor.execute("SELECT COUNT(*) as num_companies FROM riskbricks.gold.company_universe")
                metrics['company_universe'] = cursor.fetchone()[0]
            except Exception as e:
                st.warning(f"Could not fetch company universe: {str(e)[:100]}")
                metrics['company_universe'] = 0
        
        conn.close()
        return metrics
    except Exception as e:
        st.error(f"❌ **Error fetching data freshness:** {str(e)}")
        # Return empty structure so the page can still render
        return {
            'stock_prices': {'latest_date': 'N/A', 'num_symbols': 0, 'total_records': 0},
            'macro_indicators': {'latest_date': 'N/A', 'num_indicators': 0, 'total_records': 0},
            'stock_quality': {'total_records': 0, 'anomalies': 0, 'avg_quality': 0},
            'company_universe': 0
        }

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Data Freshness", "🔄 Trigger Jobs", "📋 Pipeline Status", "🔑 API Configuration"])

with tab1:
    st.markdown("## 📊 Data Freshness")
    
    # Show connection status
    test_conn = get_db_connection()
    if not test_conn:
        st.error("❌ **Database Connection Failed**")
        st.info("""
        **Troubleshooting:**
        - Check that DATABRICKS_HOST is set correctly
        - Check that DATABRICKS_WAREHOUSE_ID is configured
        - Verify DATABRICKS_TOKEN is valid
        """)
    else:
        st.success("✅ Database connected")
        test_conn.close()
    
    metrics = get_data_freshness()
    
    if not metrics:
        st.error("Unable to fetch data freshness metrics. Please check database connection.")
        st.info("Make sure the data ingestion pipeline has run successfully.")
    elif metrics:
        # Stock Prices
        st.markdown("### 📈 Stock Price Data")
        col1, col2, col3, col4 = st.columns(4)
        
        stock_data = metrics.get('stock_prices', {})
        with col1:
            latest_date = stock_data.get('latest_date', 'N/A')
            st.metric("Latest Date", str(latest_date) if latest_date != 'N/A' else 'N/A')
        with col2:
            st.metric("Symbols", f"{stock_data.get('num_symbols', 0):,}")
        with col3:
            st.metric("Total Records", f"{stock_data.get('total_records', 0):,}")
        with col4:
            latest = stock_data.get('latest_date')
            if latest:
                days_old = (datetime.now().date() - latest).days
                if days_old == 0:
                    st.metric("Freshness", "✅ Current", delta="Today")
                elif days_old <= 1:
                    st.metric("Freshness", "⚠️ 1 day old", delta=f"-{days_old} day")
                else:
                    st.metric("Freshness", f"🔴 {days_old} days old", delta=f"-{days_old} days", delta_color="inverse")
        
        # Macro Indicators
        st.markdown("### 🌍 Macro Economic Data")
        col1, col2, col3, col4 = st.columns(4)
        
        macro_data = metrics.get('macro_indicators', {})
        with col1:
            latest_date = macro_data.get('latest_date', 'N/A')
            st.metric("Latest Date", str(latest_date) if latest_date != 'N/A' else 'N/A')
        with col2:
            st.metric("Indicators", f"{macro_data.get('num_indicators', 0)}")
        with col3:
            st.metric("Total Records", f"{macro_data.get('total_records', 0):,}")
        with col4:
            latest = macro_data.get('latest_date')
            if latest:
                days_old = (datetime.now().date() - latest).days
                if days_old <= 1:
                    st.metric("Freshness", "✅ Current")
                elif days_old <= 7:
                    st.metric("Freshness", "⚠️ Weekly")
                else:
                    st.metric("Freshness", f"🔴 {days_old} days old", delta_color="inverse")
        
        # Data Quality
        st.markdown("### ✅ Data Quality (Silver Layer)")
        col1, col2, col3, col4 = st.columns(4)
        
        quality_data = metrics.get('stock_quality', {})
        with col1:
            st.metric("Total Records", f"{quality_data.get('total_records', 0):,}")
        with col2:
            st.metric("Anomalies", f"{quality_data.get('anomalies', 0):,}")
        with col3:
            anomaly_pct = (quality_data.get('anomalies', 0) / max(quality_data.get('total_records', 1), 1)) * 100
            st.metric("Anomaly Rate", f"{anomaly_pct:.2f}%")
        with col4:
            st.metric("Avg Quality Score", f"{quality_data.get('avg_quality', 0):.3f}")
        
        # Company Universe
        st.markdown("### 🏢 Company Universe")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Companies", f"{metrics.get('company_universe', 0):,}")
        
        st.markdown("---")
        
        # Refresh button
        if st.button("🔄 Refresh Metrics"):
            st.cache_data.clear()
            st.rerun()
    else:
        st.warning("Unable to fetch data freshness metrics.")

with tab2:
    st.markdown("## 🔄 Trigger Data Pipelines")
    
    st.info("""
    **Note:** These buttons will trigger Databricks jobs to run the respective notebooks.
    In production, these are configured as scheduled jobs.
    """)
    
    # Ingestion
    st.markdown("### 1️⃣ Data Ingestion")
    st.markdown("Fetch latest stock prices and macro indicators from external sources.")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("▶️ Run Ingestion", type="primary", key="btn_ingestion"):
            with st.spinner("Triggering ingestion job..."):
                job_name = "RiskBricks - Data Ingestion (Every 15 min)"
                result = trigger_job(job_name)
                
                if result:
                    st.success(f"✅ Job triggered successfully!")
                    hostname = os.getenv('DATABRICKS_HOST', '').replace('https://', '')
                    job_url = f"https://{hostname}/jobs/{result['job_id']}/runs/{result['run_id']}"
                    st.info(f"**Job ID:** {result['job_id']}")
                    st.info(f"**Run ID:** {result['run_id']}")
                    st.markdown(f"[🔗 View Job Run]({job_url})")
                    
                    # Store run ID in session state for status checking
                    if 'active_runs' not in st.session_state:
                        st.session_state.active_runs = {}
                    st.session_state.active_runs['ingestion'] = result['run_id']
    with col2:
        st.markdown("""
        **Sources:**
        - Yahoo Finance (stock prices)
        - FRED API (macro indicators)
        
        **Expected Duration:** 5-10 minutes
        """)
        
        # Show status if there's an active run
        if 'active_runs' in st.session_state and 'ingestion' in st.session_state.active_runs:
            run_id = st.session_state.active_runs['ingestion']
            status = get_run_status(run_id)
            if status:
                state = status.get('state', 'UNKNOWN')
                if state == 'RUNNING':
                    st.info(f"🔄 Job is currently running... (Run ID: {run_id})")
                elif state == 'TERMINATED':
                    result_state = status.get('result_state')
                    if result_state == 'SUCCESS':
                        st.success(f"✅ Job completed successfully! (Run ID: {run_id})")
                    else:
                        st.error(f"❌ Job failed: {result_state} (Run ID: {run_id})")
                elif state == 'PENDING':
                    st.warning(f"⏳ Job is pending... (Run ID: {run_id})")
    
    st.markdown("---")
    
    # Validation
    st.markdown("### 2️⃣ Data Validation")
    st.markdown("Validate data quality and create Silver layer tables.")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("▶️ Run Validation", key="btn_validation"):
            with st.spinner("Triggering validation job..."):
                job_name = "RiskBricks - Data Validation"
                result = trigger_job(job_name)
                
                if result:
                    st.success(f"✅ Job triggered successfully!")
                    hostname = os.getenv('DATABRICKS_HOST', '').replace('https://', '')
                    job_url = f"https://{hostname}/jobs/{result['job_id']}/runs/{result['run_id']}"
                    st.info(f"**Run ID:** {result['run_id']}")
                    st.markdown(f"[🔗 View Job Run]({job_url})")
                    
                    if 'active_runs' not in st.session_state:
                        st.session_state.active_runs = {}
                    st.session_state.active_runs['validation'] = result['run_id']
    with col2:
        st.markdown("""
        **Checks:**
        - Completeness
        - Anomaly detection
        - Quality scoring
        
        **Expected Duration:** 2-3 minutes
        """)
        
        # Show status if there's an active run
        if 'active_runs' in st.session_state and 'validation' in st.session_state.active_runs:
            run_id = st.session_state.active_runs['validation']
            status = get_run_status(run_id)
            if status and status.get('state') == 'RUNNING':
                st.info(f"🔄 Validation job running... (Run ID: {run_id})")
    
    st.markdown("---")
    
    # Analytics
    st.markdown("### 3️⃣ Risk Analytics")
    st.markdown("Compute VaR, stress tests, and sector exposures.")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("▶️ Run Analytics", key="btn_analytics"):
            with st.spinner("Triggering analytics job..."):
                job_name = "RiskBricks - Risk Analytics"
                result = trigger_job(job_name)
                
                if result:
                    st.success(f"✅ Job triggered successfully!")
                    hostname = os.getenv('DATABRICKS_HOST', '').replace('https://', '')
                    job_url = f"https://{hostname}/jobs/{result['job_id']}/runs/{result['run_id']}"
                    st.info(f"**Run ID:** {result['run_id']}")
                    st.markdown(f"[🔗 View Job Run]({job_url})")
                    
                    if 'active_runs' not in st.session_state:
                        st.session_state.active_runs = {}
                    st.session_state.active_runs['analytics'] = result['run_id']
    with col2:
        st.markdown("""
        **Calculations:**
        - Value at Risk (VaR)
        - Portfolio beta
        - Stress test scenarios
        - Sector exposures
        
        **Expected Duration:** 3-5 minutes
        """)
        
        # Show status if there's an active run
        if 'active_runs' in st.session_state and 'analytics' in st.session_state.active_runs:
            run_id = st.session_state.active_runs['analytics']
            status = get_run_status(run_id)
            if status and status.get('state') == 'RUNNING':
                st.info(f"🔄 Analytics job running... (Run ID: {run_id})")
    
    st.markdown("---")
    
    # Full Pipeline
    st.markdown("### 🚀 Run Full Pipeline")
    st.markdown("Execute all three steps sequentially: Ingestion → Validation → Analytics")
    
    if st.button("▶️ Run Full Pipeline", type="primary", key="btn_full_pipeline"):
        st.info("⚠️ **Full Pipeline mode:**")
        st.markdown("""
        To run the full pipeline with proper dependencies, create a multi-task job in Databricks:
        
        1. Go to **Workflows → Jobs → Create Job**
        2. Add 3 tasks with dependencies:
           - Task 1: `01_data_ingestion` 
           - Task 2: `02_data_validation` (depends on Task 1)
           - Task 3: `03_risk_analytics` (depends on Task 2)
        3. Or trigger each step manually above in sequence
        
        **For now, use the individual buttons above to run each step.**
        """)
    
    st.markdown("---")
    st.markdown("""
    **💡 Production Note:**
    
    In a production deployment, these jobs would:
    - Be configured as Databricks workflows
    - Run on a schedule (e.g., every 15 minutes for ingestion)
    - Have proper error handling and notifications
    - Log to Delta tables for audit trails
    """)

with tab3:
    st.markdown("## 📋 Pipeline Status")
    
    st.markdown("### Recent Job Runs")
    
    # Mock data for demonstration
    # In production, this would query Databricks Jobs API
    job_runs = pd.DataFrame({
        'job_name': [
            '01_data_ingestion',
            '02_data_validation',
            '03_risk_analytics',
            '01_data_ingestion',
            '02_data_validation'
        ],
        'status': ['✅ Success', '✅ Success', '✅ Success', '✅ Success', '⚠️ Warning'],
        'start_time': [
            datetime.now() - timedelta(hours=1),
            datetime.now() - timedelta(hours=1, minutes=5),
            datetime.now() - timedelta(hours=1, minutes=10),
            datetime.now() - timedelta(hours=4),
            datetime.now() - timedelta(hours=4, minutes=5)
        ],
        'duration': ['8m 32s', '2m 15s', '4m 48s', '8m 10s', '2m 30s'],
        'records_processed': ['1,114,104', '1,114,104', '3 managers', '1,112,890', '1,112,890']
    })
    
    st.dataframe(
        job_runs.style.format({'start_time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')}),
        use_container_width=True
    )
    
    st.markdown("---")
    
    st.markdown("### Pipeline Health")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Success Rate (24h)", "96.7%", delta="2.3%")
    
    with col2:
        st.metric("Avg Duration", "15m 25s", delta="-2m 10s")
    
    with col3:
        st.metric("Last Success", "1 hour ago")
    
    st.markdown("---")
    
    st.markdown("""
    **💡 Production Note:**
    
    In production, this section would show:
    - Real-time job status from Databricks Jobs API
    - Historical trends and success rates
    - Error logs and stack traces
    - Alerting configuration
    """)

with tab4:
    st.markdown("## 🔑 API Configuration")
    
    st.markdown("""
    ### Data Source Configuration
    
    RiskBricks uses these data sources:
    """)
    
    # FRED API
    with st.expander("📊 FRED API (Federal Reserve Economic Data)", expanded=True):
        st.markdown("""
        **Purpose:** Macro economic indicators
        
        **Indicators Retrieved:**
        - Federal Funds Rate (FEDFUNDS)
        - VIX Volatility Index (VIXCLS)
        - 10-Year Treasury Yield (DGS10)
        - Unemployment Rate (UNRATE)
        - GDP (GDP)
        - CPI Inflation (CPIAUCSL)
        """)
        
        fred_key = os.getenv("FRED_API_KEY", "Not configured")
        if fred_key != "Not configured":
            st.success("✅ API Key configured in Databricks Secrets")
            st.code(f"Key: {fred_key[:8]}...{fred_key[-4:]}")
        else:
            st.warning("⚠️ API Key not found")
            st.markdown("""
            **Setup:**
            1. Get API key from https://fredaccount.stlouisfed.org/apikey
            2. Store in Databricks Secrets:
               ```bash
               databricks secrets put-secret riskbricks fred_api_key
               ```
            """)
    
    # Yahoo Finance
    with st.expander("📈 Yahoo Finance (yfinance)", expanded=True):
        st.markdown("""
        **Purpose:** Stock price data (OHLCV)
        
        **Data Retrieved:**
        - Daily OHLC prices
        - Adjusted close prices
        - Trading volumes
        - 12 years of historical data
        
        **Note:** Yahoo Finance API is free and doesn't require an API key.
        Uses the `yfinance` Python library.
        """)
        st.success("✅ No API key required")
    
    st.markdown("---")
    
    # Agent Endpoint Configuration
    with st.expander("🤖 Agent Bricks Endpoint"):
        st.markdown("""
        **Current Endpoint:**
        """)
        endpoint = os.getenv("AGENT_ENDPOINT", "Not configured")
        st.code(endpoint)
        
        if st.button("🧪 Test Endpoint"):
            st.info("Sending test query to agent...")
            st.success("✅ Agent responded successfully (mock)")
    
    st.markdown("---")
    
    st.markdown("""
    ### 🔒 Security Best Practices
    
    ✅ **All API keys are stored in Databricks Secrets**
    - Never hardcoded in notebooks or code
    - Encrypted at rest
    - Access controlled by workspace permissions
    
    ✅ **Agent endpoint uses token authentication**
    - Databricks personal access token required
    - Can be rotated without code changes
    
    ✅ **Unity Catalog for data governance**
    - Fine-grained access control
    - Audit logging for all data access
    - Data lineage tracking
    """)

# Footer
st.markdown("---")
st.markdown("""
### 📖 Data Pipeline Architecture

**Medallion Architecture:**

1. **Bronze Layer** (Raw Data)
   - Stock prices from Yahoo Finance
   - Macro indicators from FRED
   - 12 years of historical data

2. **Silver Layer** (Validated Data)
   - Data quality checks
   - Anomaly detection
   - Clean, curated datasets

3. **Gold Layer** (Business-Ready Analytics)
   - Portfolio risk metrics
   - Stress test results
   - Sector exposures
   - Ready for AI agent consumption

**Scheduled Jobs:**
- Ingestion: Every 15 minutes (market hours)
- Validation: Triggered after ingestion
- Analytics: Triggered after validation
- End-to-end latency: < 20 minutes
""")
