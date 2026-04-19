"""
RiskBricks - Simple Test Version
"""

import streamlit as st

st.set_page_config(
    page_title="RiskBricks",
    page_icon="📊",
    layout="wide"
)

st.title("📊 RiskBricks - Test Version")
st.markdown("## ✅ App is working!")

st.success("If you can see this, the Streamlit app is deployed successfully!")

st.markdown("""
### 🎯 Next Steps:

1. ✅ Basic Streamlit app is working
2. Now we can add database connections
3. Then add the full features

### 📊 System Info:

- Streamlit version: Check sidebar
- Python version: 3.x
- Status: Running
""")

st.sidebar.success("✅ Sidebar working!")
st.sidebar.markdown("### Test Navigation")
st.sidebar.markdown("If this appears, multi-page should work too!")
