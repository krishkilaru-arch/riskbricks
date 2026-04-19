"""
RiskBricks - Minimal Test App
This is the simplest possible Streamlit app to verify deployment works
"""

import streamlit as st

st.set_page_config(
    page_title="RiskBricks",
    page_icon="📊",
    layout="wide"
)

st.title("📊 RiskBricks")
st.success("✅ App is running successfully!")

st.markdown("""
## 🎉 Deployment Successful!

If you can see this page, the Databricks App deployment is working correctly.

### ✅ What's Working:
- Streamlit is installed and running
- The app is accessible via URL
- Basic routing is functional

### 📝 Next Steps:
1. Add database connections
2. Add the full 5-page interface
3. Connect to Unity Catalog
4. Enable AI Agent chat

### 🔧 System Info:
- **App Name:** riskbricks-web-app
- **Status:** Running
- **Port:** 8501
- **Framework:** Streamlit
""")

with st.sidebar:
    st.success("✅ Sidebar working!")
    st.markdown("### Quick Links")
    st.markdown("- [Databricks Workspace](#)")
    st.markdown("- [Agent Bricks](#/ml/agents)")
