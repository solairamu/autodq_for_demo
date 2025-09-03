# data_loader.py

import pandas as pd
from databricks import sql
import streamlit as st
from datetime import datetime
import os
from config import DEFAULT_SCHEMA
from environment_detector import environment_detector

@st.cache_data(show_spinner="Connecting to Databricks...")
def load_data_from_databricks():
    schema = st.session_state["METADATA"]["schema"]
    
    try:
        # Use environment detector for connection configuration
        connection_config = environment_detector.get_connection_config()
        
        if connection_config["use_automatic_auth"]:
            # Use Databricks runtime connection (automatic authentication)
            import databricks.sql as databricks_sql
            with databricks_sql.connect() as connection:
                df = pd.read_sql(f"SELECT * FROM kdataai.{schema}.gx_validation_results_cleaned_combined", connection)
        else:
            # Check if setup is required
            if connection_config.get("requires_setup", False):
                env_info = environment_detector.get_setup_instructions()
                st.error("⚠️ AutoDQ needs to be configured for your Databricks workspace.")
                st.info("🔧 Please run the setup wizard to configure your connection:")
                st.code("streamlit run setup_wizard.py")
                return pd.DataFrame()
            
            # Use explicit connection parameters
            params = connection_config["connection_params"]
            
            if not all([params.get("server_hostname"), params.get("http_path"), params.get("access_token")]):
                st.error("❌ Missing Databricks connection parameters. Please run the setup wizard.")
                return pd.DataFrame()
            
            with sql.connect(
                server_hostname=params["server_hostname"],
                http_path=params["http_path"],
                access_token=params["access_token"]
            ) as connection:
                df = pd.read_sql(f"SELECT * FROM kdataai.{schema}.gx_validation_results_cleaned_combined", connection)
        
        return df
    except Exception as e:
        _display_connection_error(e)
        return pd.DataFrame()

def _display_connection_error(error):
    """Display helpful error messages based on the type of connection error"""
    error_str = str(error).lower()
    
    st.error(f"Failed to connect to Databricks: {error}")
    
    # Provide specific troubleshooting based on error type
    if "authentication" in error_str or "token" in error_str:
        st.info("🔑 **Authentication Issue**: Your access token may be expired or invalid.")
        st.markdown("**Solutions:**\n- Generate a new access token in Databricks\n- Run the setup wizard to update your configuration")
        
    elif "warehouse" in error_str or "endpoint" in error_str:
        st.info("🏭 **SQL Warehouse Issue**: Your SQL warehouse may be stopped or the path is incorrect.")
        st.markdown("**Solutions:**\n- Start your SQL warehouse in Databricks\n- Verify the HTTP path in your configuration")
        
    elif "network" in error_str or "connection" in error_str:
        st.info("🌐 **Network Issue**: Cannot reach your Databricks workspace.")
        st.markdown("**Solutions:**\n- Check your workspace URL\n- Verify network connectivity\n- Check firewall settings")
        
    else:
        st.info("🔧 **General Issue**: Please check your configuration.")
        
    # Always offer the setup wizard as a solution
    st.markdown("---")
    st.info("💡 **Need help?** Run the setup wizard to reconfigure your connection:")
    st.code("streamlit run setup_wizard.py")

def _is_running_in_databricks():
    """Check if the app is running in a Databricks environment"""
    # This function is kept for backwards compatibility
    env_config = environment_detector.detect_environment()
    return env_config["environment_type"] in ["databricks_runtime", "databricks_lakehouse_app"]

def initialize_metadata(df):
    if df.empty or 'Table' not in df.columns:
        return
    if not st.session_state["METADATA"]["tables"]:
        discovered_tables = sorted(df['Table'].dropna().unique().tolist())
        st.session_state["METADATA"]["tables"] = discovered_tables
        st.session_state["table_scope"] = {t: True for t in discovered_tables}
        st.rerun()
