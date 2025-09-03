# data_loader.py

import pandas as pd
from databricks import sql
import streamlit as st
from datetime import datetime
import os
from config import DEFAULT_SCHEMA

@st.cache_data(show_spinner="Connecting to Databricks...")
def load_data_from_databricks():
    schema = st.session_state["METADATA"]["schema"]
    
    try:
        # Check if running in Databricks environment
        if _is_running_in_databricks():
            # Use Databricks runtime connection (automatic authentication)
            import databricks.sql as databricks_sql
            with databricks_sql.connect() as connection:
                df = pd.read_sql(f"SELECT * FROM kdataai.{schema}.gx_validation_results_cleaned_combined", connection)
        else:
            # Use explicit connection parameters for local development
            server_hostname = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
            http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
            access_token = os.getenv("DATABRICKS_TOKEN", "")
            
            if not all([server_hostname, http_path, access_token]):
                raise ValueError("Missing required Databricks connection parameters. Please check your environment variables.")
            
            with sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            ) as connection:
                df = pd.read_sql(f"SELECT * FROM kdataai.{schema}.gx_validation_results_cleaned_combined", connection)
        
        return df
    except Exception as e:
        st.error(f"Failed to connect to Databricks. Please check settings. Error: {e}")
        return pd.DataFrame()

def _is_running_in_databricks():
    """Check if the app is running in a Databricks environment"""
    return (
        os.getenv("DATABRICKS_RUNTIME_VERSION") is not None or
        os.getenv("DATABRICKS_WORKSPACE_URL") is not None or
        os.path.exists("/databricks/driver")
    )

def initialize_metadata(df):
    if df.empty or 'Table' not in df.columns:
        return
    if not st.session_state["METADATA"]["tables"]:
        discovered_tables = sorted(df['Table'].dropna().unique().tolist())
        st.session_state["METADATA"]["tables"] = discovered_tables
        st.session_state["table_scope"] = {t: True for t in discovered_tables}
        st.rerun()
