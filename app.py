import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import base64
import os
import sys

# Import Config and Modules
from config import DEFAULT_SCHEMA, DEFAULT_REFRESH_INTERVAL
from data_loader import load_data_from_databricks, initialize_metadata
import product_overview, dq_dashboard, data_cleaning, anomaly_detection, schema_inference, coverage, alerts, dq_tracker, settings
import smart_rule_assistant, data_intelligence

# ⚙️ Page Setup
st.set_page_config(layout="wide", page_title="AutoDQ For Databricks")

# Sidebar Styling
st.markdown("""
    <style>
    section[data-testid="stSidebar"] div[data-testid="stRadio"] {
        padding-top: 0px;
        margin-top: -15px;
    }
    #bottom-kdata-logo {
        position: fixed;
        bottom: 20px;
        left: 20px;
        z-index: 999999;
    }
    </style>
""", unsafe_allow_html=True)

# 📷 Logo Encoding with Caching
@st.cache_data
def get_base64_image(path):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except FileNotFoundError:
        st.warning(f"Logo file not found: {path}")
        return None

# Load logo from images folder (cached)
logo_base64 = get_base64_image("./images/Only logo.png")

# 📍 Sidebar Navigation (updated order)
st.sidebar.markdown("<h4><b>Go To</b></h4>", unsafe_allow_html=True)
selected_page = st.sidebar.radio(
    label="",
    options=[
        "About AutoDQ",
        "Settings",
        "Validation Dashboard",
        "Smart Rule Assistant",
        "Data Intelligence Hub",
        "Check Coverage",
        "Alert Feed",
        "DQ Action Tracker"
    ]
)

# 🔄 Initialize Session State
default_states = {
    "METADATA": {"tables": [], "schema": DEFAULT_SCHEMA},
    "refresh_interval": DEFAULT_REFRESH_INTERVAL,
    "refresh_mode": "Interval-Based",
    "specific_refresh_datetime": None,
    "action_tracker": pd.DataFrame(),
    "last_refresh": datetime.now(),
    "table_scope": {}
}
for key, value in default_states.items():
    if key not in st.session_state:
        st.session_state[key] = value

# ⏰ Auto Refresh Logic
now = datetime.now()
need_refresh = False
if st.session_state.refresh_mode == "Interval-Based":
    if now - st.session_state.last_refresh > timedelta(minutes=st.session_state.refresh_interval):
        need_refresh = True
elif st.session_state.refresh_mode == "Specific Date/Time":
    if st.session_state.specific_refresh_datetime and now >= st.session_state.specific_refresh_datetime and st.session_state.last_refresh < st.session_state.specific_refresh_datetime:
        need_refresh = True

# 🧑‍💻 Load Data
if "df" not in st.session_state or need_refresh:
    try:
        st.session_state["df"] = load_data_from_databricks()
        st.session_state.last_refresh = now
        initialize_metadata(st.session_state["df"])
    except Exception as e:
        st.error(f"Failed to load data from Databricks: {e}")
        st.session_state["df"] = pd.DataFrame()

# 🗐 Manual Refresh Button
if selected_page != "About AutoDQ":
    if st.button("Refresh from Databricks"):
        try:
            st.session_state["df"] = load_data_from_databricks()
            st.session_state["last_refresh"] = now
            initialize_metadata(st.session_state["df"])
            st.rerun()
        except Exception as e:
            st.error(f"Failed to refresh data: {e}")

# 🪜 Preprocess DataFrame
df = st.session_state.get("df", pd.DataFrame())
if not df.empty:
    df.columns = df.columns.str.strip()
    for col in ['Table', 'Column']:
        if col in df.columns:
            df[col] = df[col].str.strip()
    df['Table'] = df['Table'].str.lower()
    df['Table.Column'] = df['Table'].astype(str) + '.' + df['Column'].astype(str)
    if "Run_Timestamp" in df.columns:
        df["Run_Timestamp"] = pd.to_datetime(df["Run_Timestamp"], errors="coerce")
        df.dropna(subset=["Run_Timestamp"], inplace=True)
else:
    if selected_page != "About AutoDQ":
        st.warning("No data loaded. Please check settings or refresh from Databricks.")

# 🌐 Render Page Based on Selection
if selected_page == "About AutoDQ":
    st.markdown("""
    <div style='text-align: center; margin-bottom: 10px; line-height: 1.2;'>
        <div style='font-size: 48px; font-weight: bold; color: #222;'>AutoDQ</div>
        <div style='font-size: 20px; color: #777; margin-top: 4px;'>by KData</div>
    </div>
    <hr style='margin-top: -5px; margin-bottom: 30px; border: 1px solid #ccc;'>
    """, unsafe_allow_html=True)
    product_overview.render()
elif selected_page == "Settings":
    settings.render()
elif selected_page == "Validation Dashboard":
    dq_dashboard.render()
elif selected_page == "Smart Rule Assistant":
    smart_rule_assistant.render(df)
elif selected_page == "Data Intelligence Hub":
    data_intelligence.render(df)
elif selected_page == "Check Coverage":
    coverage.render(df)
elif selected_page == "Alert Feed":
    alerts.render(df)
elif selected_page == "DQ Action Tracker":
    dq_tracker.render()

# 📌 KData Logo Footer (commented out for now)
if logo_base64:
    st.markdown(
        f"""
        <div id="bottom-kdata-logo">
            <img src="data:image/png;base64,{logo_base64}" width="100">
        </div>
        """,
        unsafe_allow_html=True
    )

# Standard Streamlit app entry point
if __name__ == "__main__":
    # Run with: streamlit run app.py
    pass
