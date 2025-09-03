import streamlit as st
from datetime import datetime
from config import DQ_STATUS_OPTIONS, DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
import os

from databricks import sql
import requests
import pandas as pd

# ---- Databricks Config from Environment Variables ----
def get_databricks_connection_params():
    """Get Databricks connection parameters from environment variables"""
    host = os.getenv("DATABRICKS_HOST", DATABRICKS_HOST)
    token = os.getenv("DATABRICKS_TOKEN", DATABRICKS_TOKEN)
    http_path = os.getenv("DATABRICKS_HTTP_PATH", DATABRICKS_HTTP_PATH)
    job_id = os.getenv("DATABRICKS_JOB_ID", "60519178052060")
    
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks connection parameters")
    
    return host, token, http_path, job_id

DELTA_SCHEMA = os.getenv("DEFAULT_SCHEMA", "multitable_logistics")
VALIDATION_TABLE = "gx_validation_results_cleaned_combined"
RULE_METADATA_TABLE = "silver_layer_rule_metadata_DQ"

# ---- Connection Helper ----
def get_databricks_connection():
    """Get a Databricks SQL connection"""
    host, token, http_path, _ = get_databricks_connection_params()
    
    return sql.connect(
        server_hostname=host.replace("https://", ""),
        http_path=http_path,
        access_token=token
    )

# ---- Metadata Fetchers ----
def fetch_generated_results():
    """Fetch validation results from Databricks"""
    try:
        conn = get_databricks_connection()
        query = f"SELECT * FROM {DELTA_SCHEMA}.{VALIDATION_TABLE}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to fetch validation results: {e}")
        return pd.DataFrame()

def fetch_rule_metadata():
    """Fetch rule metadata from Databricks"""
    try:
        conn = get_databricks_connection()
        query = f"SELECT * FROM {DELTA_SCHEMA}.{RULE_METADATA_TABLE}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to fetch rule metadata: {e}")
        return pd.DataFrame()

def get_all_schemas():
    """Get all available schemas from Databricks"""
    try:
        conn = get_databricks_connection()
        with conn.cursor() as cursor:
            cursor.execute("SHOW SCHEMAS")
            results = cursor.fetchall()
        conn.close()
        return [row[0] for row in results]
    except Exception as e:
        st.error(f"Failed to fetch schemas: {e}")
        return [DELTA_SCHEMA]

def get_tables_in_schema(schema_name):
    """Get all tables in a specific schema"""
    try:
        conn = get_databricks_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES IN {schema_name}")
            results = cursor.fetchall()
        conn.close()
        return [row[1] for row in results]  # Table name is in the second column
    except Exception as e:
        st.error(f"Failed to fetch tables in schema {schema_name}: {e}")
        return []

def get_columns_in_table(schema_name, table_name):
    conn = get_databricks_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE TABLE {schema_name}.{table_name}")
        results = cursor.fetchall()
    conn.close()
    return [{"Column": row[0], "DataType": row[1]} for row in results if row[0]]

def trigger_databricks_job():
    endpoint = f"{DATABRICKS_HOST}/api/2.0/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    payload = {"job_id": DATABRICKS_JOB_ID}
    response = requests.post(endpoint, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json().get("run_id")
    else:
        st.error(f"Failed to trigger job: {response.text}")
        return None

# ---- Render Settings UI ----
def render():
    """Render the settings page"""
    st.subheader("⚙️ Settings & Configuration")
    
    # ---- CONNECTION STATUS ----
    st.markdown("### 🔗 Databricks Connection")
    
    with st.expander("Connection Status", expanded=True):
        try:
            host, token, http_path, job_id = get_databricks_connection_params()
            
            col1, col2 = st.columns(2)
            with col1:
                st.success("✅ Connection Parameters Configured")
                st.info(f"**Host:** {host[:30]}..." if len(host) > 30 else host)
                st.info(f"**HTTP Path:** {http_path}")
                
            with col2:
                # Test connection
                if st.button("🧪 Test Connection"):
                    with st.spinner("Testing connection..."):
                        try:
                            conn = get_databricks_connection()
                            with conn.cursor() as cursor:
                                cursor.execute("SELECT 1 as test")
                                result = cursor.fetchone()
                            conn.close()
                            
                            if result:
                                st.success("✅ Connection test successful!")
                            else:
                                st.error("❌ Connection test failed - no result returned")
                        except Exception as e:
                            st.error(f"❌ Connection test failed: {e}")
                            
        except Exception as e:
            st.error(f"❌ Connection configuration error: {e}")
            st.warning("Please check your environment variables for Databricks connection parameters.")
    
    # ---- ENVIRONMENT VARIABLES INFO ----
    with st.expander("Environment Variables", expanded=False):
        st.markdown("""
        **Required Environment Variables:**
        
        - `DATABRICKS_HOST` - Your Databricks workspace URL
        - `DATABRICKS_TOKEN` - Personal access token or service principal token
        - `DATABRICKS_HTTP_PATH` - SQL warehouse HTTP path
        
        **Optional Environment Variables:**
        
        - `DEFAULT_SCHEMA` - Default schema to use (default: multitable_logistics)
        - `DEFAULT_REFRESH_INTERVAL` - Refresh interval in minutes (default: 10)
        - `DATABRICKS_JOB_ID` - Job ID for smart rule execution
        """)
        
        # Show current values (masked for security)
        st.markdown("**Current Configuration:**")
        env_vars = {
            "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "Not set"),
            "DATABRICKS_TOKEN": "***" + os.getenv("DATABRICKS_TOKEN", "")[-4:] if os.getenv("DATABRICKS_TOKEN") else "Not set",
            "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH", "Not set"),
            "DEFAULT_SCHEMA": os.getenv("DEFAULT_SCHEMA", "multitable_logistics"),
            "DEFAULT_REFRESH_INTERVAL": os.getenv("DEFAULT_REFRESH_INTERVAL", "10"),
        }
        
        for key, value in env_vars.items():
            st.text(f"{key}: {value}")
    
    # ---- SCHEMA CONFIGURATION ----
    st.markdown("### 🗄️ Schema & Data Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Available Schemas**")
        try:
            schemas = get_all_schemas()
            if schemas:
                for schema in schemas[:10]:  # Show first 10 schemas
                    st.text(f"📁 {schema}")
                if len(schemas) > 10:
                    st.text(f"... and {len(schemas) - 10} more")
            else:
                st.warning("No schemas found or connection issue")
        except Exception as e:
            st.error(f"Failed to load schemas: {e}")
    
    with col2:
        st.markdown("**Current Schema Tables**")
        current_schema = os.getenv("DEFAULT_SCHEMA", "multitable_logistics")
        
        try:
            tables = get_tables_in_schema(current_schema)
            if tables:
                for table in tables[:10]:  # Show first 10 tables
                    st.text(f"📋 {table}")
                if len(tables) > 10:
                    st.text(f"... and {len(tables) - 10} more")
            else:
                st.warning(f"No tables found in schema '{current_schema}'")
        except Exception as e:
            st.error(f"Failed to load tables: {e}")
    
    # ---- REFRESH SETTINGS ----
    st.markdown("### 🔄 Refresh Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        refresh_modes = ["Interval-Based", "Specific Date/Time", "Manual Only"]
        current_mode = st.session_state.get("refresh_mode", "Interval-Based")
        
        new_mode = st.selectbox(
            "Refresh Mode",
            refresh_modes,
            index=refresh_modes.index(current_mode)
        )
        
        if new_mode != current_mode:
            st.session_state["refresh_mode"] = new_mode
    
    with col2:
        if st.session_state.get("refresh_mode") == "Interval-Based":
            current_interval = st.session_state.get("refresh_interval", 10)
            new_interval = st.number_input(
                "Refresh Interval (minutes)",
                min_value=1,
                max_value=1440,  # 24 hours
                value=current_interval
            )
            
            if new_interval != current_interval:
                st.session_state["refresh_interval"] = new_interval
                
        elif st.session_state.get("refresh_mode") == "Specific Date/Time":
            specific_time = st.datetime_input(
                "Next Refresh Time",
                value=st.session_state.get("specific_refresh_datetime", datetime.now())
            )
            st.session_state["specific_refresh_datetime"] = specific_time
    
    # ---- DATA QUALITY SETTINGS ----
    st.markdown("### 📊 Data Quality Settings")
    
    with st.expander("Validation Rules Configuration", expanded=False):
        st.markdown("""
        **Available Rule Types:**
        - No Nulls (High Priority)
        - Unique Values (High Priority)  
        - Primary Key Present (High Priority)
        - Foreign Key Valid (High Priority)
        - Range OK (Normal Priority)
        - Valid Type (Normal Priority)
        - Format Match (Normal Priority)
        - Column Present (Normal Priority)
        - Allowed Values (Normal Priority)
        - Valid Date (Normal Priority)
        """)
        
        st.markdown("**Status Options:**")
        for status in DQ_STATUS_OPTIONS:
            st.text(f"• {status}")
    
    # ---- EXPORT/IMPORT SETTINGS ----
    st.markdown("### 📤 Export/Import")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📥 Export Current Configuration"):
            config_data = {
                "refresh_mode": st.session_state.get("refresh_mode"),
                "refresh_interval": st.session_state.get("refresh_interval"),
                "schema": os.getenv("DEFAULT_SCHEMA"),
                "export_timestamp": datetime.now().isoformat()
            }
            
            config_json = pd.DataFrame([config_data]).to_json(orient="records", indent=2)
            st.download_button(
                label="💾 Download Configuration",
                data=config_json,
                file_name=f"autodq_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    with col2:
        st.markdown("**Import Configuration**")
        uploaded_file = st.file_uploader(
            "Upload configuration file",
            type=['json'],
            help="Upload a previously exported configuration file"
        )
        
        if uploaded_file is not None:
            try:
                config_data = pd.read_json(uploaded_file)
                st.success("Configuration file loaded successfully!")
                st.json(config_data.to_dict('records')[0])
            except Exception as e:
                st.error(f"Failed to load configuration: {e}")
    
    # ---- SYSTEM INFO ----
    st.markdown("### ℹ️ System Information")
    
    with st.expander("Application Details", expanded=False):
        system_info = {
            "Application": "AutoDQ for Databricks",
            "Version": "1.0.0",
            "Framework": "Streamlit",
            "Python Version": "3.9+",
            "Last Refresh": st.session_state.get("last_refresh", "Never").strftime("%Y-%m-%d %H:%M:%S") if st.session_state.get("last_refresh") != "Never" else "Never",
            "Current Schema": os.getenv("DEFAULT_SCHEMA", "multitable_logistics"),
            "Port": os.getenv("DATABRICKS_APP_PORT", "8501")
        }
        
        for key, value in system_info.items():
            st.text(f"{key}: {value}")
    
    # ---- RESET SETTINGS ----
    st.markdown("### 🔄 Reset Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Reset to Defaults", type="secondary"):
            # Reset session state to defaults
            default_states = {
                "refresh_mode": "Interval-Based",
                "refresh_interval": 10,
                "specific_refresh_datetime": None,
            }
            
            for key, value in default_states.items():
                st.session_state[key] = value
            
            st.success("Settings reset to defaults!")
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear All Data", type="secondary"):
            # Clear all session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            
            st.success("All data cleared!")
            st.rerun()
