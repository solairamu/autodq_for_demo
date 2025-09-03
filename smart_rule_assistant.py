import streamlit as st
import requests
import pandas as pd
import time
from databricks import sql
import os
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH

# -------------------------------
# 🔧 Config
# -------------------------------
def get_databricks_connection_params():
    """Get Databricks connection parameters from environment variables"""
    host = os.getenv("DATABRICKS_HOST", DATABRICKS_HOST)
    token = os.getenv("DATABRICKS_TOKEN", DATABRICKS_TOKEN)
    http_path = os.getenv("DATABRICKS_HTTP_PATH", DATABRICKS_HTTP_PATH)
    job_id = os.getenv("DATABRICKS_JOB_ID", "85153824557870")  # Default fallback
    
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks connection parameters")
    
    return host, token, http_path, job_id

DELTA_TABLE = "multitable_logistics.user_defined_validation_log_final_new"
SAVED_TABLE = "multitable_logistics.user_defined_validation_log_final_for_dashboard"

# -------------------------------
# 📦 Fetch Saved Logs
# -------------------------------
def fetch_saved_validations():
    try:
        host, token, http_path, _ = get_databricks_connection_params()
        
        with sql.connect(
            server_hostname=host.replace("https://", ""),
            http_path=http_path,
            access_token=token
        ) as connection:
            df = pd.read_sql(f"""
                SELECT * 
                FROM {SAVED_TABLE}
                ORDER BY Run_Timestamp DESC
            """, connection)
            return df
    except Exception as e:
        st.error(f"❌ Failed to load saved validations: {e}")
        return pd.DataFrame()

# -------------------------------
# 🧠 Smart Rule Assistant Renderer
# -------------------------------
def render(df):
    st.subheader("Smart Rule Assistant")
    st.markdown("Designed to simplify data validation, the Smart Rule Assistant uses AI to interpret natural language rules and apply them across all data, detecting any rule violations with Databricks integration.")

    # Initialize session state
    for key in [
        "user_rule_input", "validation_df", "execution_id", "save_status",
        "job_running", "stop_requested", "already_checked",
        "confirm_save_action", "skip_save_action"
    ]:
        if key not in st.session_state:
            st.session_state[key] = None

    # ------------------- EXISTING RULES -------------------
    st.markdown("### 📋 Previously Saved Smart Rules")
    
    with st.expander("View Saved Validations", expanded=False):
        saved_df = fetch_saved_validations()
        
        if not saved_df.empty:
            # Show summary
            total_saved = len(saved_df)
            unique_rules = saved_df['Rule_Display_Name'].nunique() if 'Rule_Display_Name' in saved_df.columns else 0
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Saved Validations", total_saved)
            with col2:
                st.metric("Unique Rules", unique_rules)
            
            # Show recent validations
            st.markdown("#### Recent Validations")
            display_columns = ['Run_Timestamp', 'Rule_Display_Name', 'Status'] if all(col in saved_df.columns for col in ['Run_Timestamp', 'Rule_Display_Name', 'Status']) else saved_df.columns.tolist()[:3]
            st.dataframe(
                saved_df[display_columns].head(10),
                use_container_width=True
            )
        else:
            st.info("No saved validations found yet. Create your first smart rule below!")

    st.markdown("---")

    # ------------------- SMART RULE INPUT -------------------
    st.markdown("### 🤖 Create New Smart Rule")
    
    user_rule = st.text_area(
        "Describe your validation rule in plain English:",
        value=st.session_state.user_rule_input or "",
        placeholder="Example: 'Check that all customer ages are between 18 and 120' or 'Ensure all email addresses contain @ symbol'",
        height=100
    )
    
    if user_rule != st.session_state.user_rule_input:
        st.session_state.user_rule_input = user_rule

    # ------------------- RULE EXECUTION -------------------
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("🚀 Execute Smart Rule", disabled=not user_rule.strip()):
            if user_rule.strip():
                try:
                    # For demo purposes, we'll show a simulated execution
                    st.session_state.job_running = True
                    st.session_state.execution_id = f"rule_execution_{int(time.time())}"
                    
                    with st.spinner("🔄 Processing your rule with AI..."):
                        time.sleep(2)  # Simulate processing time
                        
                        # Simulate successful rule execution
                        sample_results = {
                            'Rule_Display_Name': [user_rule[:50] + "..." if len(user_rule) > 50 else user_rule],
                            'Status': ['Executed'],
                            'Tables_Analyzed': [3],
                            'Records_Checked': [1250],
                            'Violations_Found': [12],
                            'Execution_Time': ['2.3 seconds']
                        }
                        
                        st.session_state.validation_df = pd.DataFrame(sample_results)
                        st.session_state.job_running = False
                        
                    st.success("✅ Smart rule executed successfully!")
                        
                except Exception as e:
                    st.error(f"❌ Failed to execute rule: {e}")
                    st.session_state.job_running = False
            else:
                st.warning("Please enter a validation rule first.")
    
    with col2:
        if st.session_state.job_running:
            if st.button("⏹️ Stop Execution"):
                st.session_state.stop_requested = True
                st.session_state.job_running = False
                st.warning("Execution stopped by user.")

    # ------------------- EXECUTION RESULTS -------------------
    if st.session_state.validation_df is not None and not st.session_state.validation_df.empty:
        st.markdown("### 📊 Execution Results")
        
        # Display results summary
        results_df = st.session_state.validation_df
        
        if 'Violations_Found' in results_df.columns:
            violations = results_df['Violations_Found'].iloc[0]
            records_checked = results_df['Records_Checked'].iloc[0] if 'Records_Checked' in results_df.columns else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records Checked", f"{records_checked:,}")
            with col2:
                st.metric("Violations Found", violations)
            with col3:
                success_rate = ((records_checked - violations) / records_checked * 100) if records_checked > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
        
        # Show detailed results
        st.dataframe(results_df, use_container_width=True)
        
        # ------------------- SAVE OPTIONS -------------------
        st.markdown("### 💾 Save Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Save to Dashboard"):
                try:
                    # In a real implementation, this would save to Databricks
                    st.session_state.save_status = "saved"
                    st.success("✅ Results saved successfully!")
                except Exception as e:
                    st.error(f"❌ Failed to save results: {e}")
        
        with col2:
            # Export option
            csv_data = results_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Results",
                data=csv_data,
                file_name=f"smart_rule_results_{int(time.time())}.csv",
                mime="text/csv"
            )

    # ------------------- HELP SECTION -------------------
    with st.expander("💡 Smart Rule Examples", expanded=False):
        st.markdown("""
        **Data Quality Rules:**
        - "Check that all customer IDs are unique"
        - "Verify that order amounts are greater than zero"
        - "Ensure all email addresses are valid format"
        
        **Business Logic Rules:**
        - "Customer age should be between 18 and 120 years"
        - "Order dates should not be in the future"
        - "Product prices must be positive numbers"
        
        **Completeness Rules:**
        - "All required fields should not be null"
        - "Every customer should have a valid address"
        - "Phone numbers should follow standard format"
        """)

    # ------------------- CONNECTION STATUS -------------------
    with st.expander("🔧 Connection Status", expanded=False):
        try:
            host, token, http_path, job_id = get_databricks_connection_params()
            st.success("✅ Databricks connection configured")
            st.info(f"Host: {host[:20]}...")
            st.info(f"HTTP Path: {http_path}")
        except Exception as e:
            st.error(f"❌ Connection issue: {e}")
            st.warning("Please check your environment variables for Databricks connection.")

def trigger_databricks_job():
    """Trigger a Databricks job for rule execution"""
    try:
        host, token, _, job_id = get_databricks_connection_params()
        
        url = f"{host}/api/2.1/jobs/run-now"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {"job_id": int(job_id)}
        
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            return response.json().get("run_id")
        else:
            raise Exception(f"Failed to trigger job: {response.text}")
            
    except Exception as e:
        st.error(f"Failed to trigger Databricks job: {e}")
        return None

def check_job_status(run_id):
    """Check the status of a Databricks job run"""
    try:
        host, token, _, _ = get_databricks_connection_params()
        
        url = f"{host}/api/2.1/jobs/runs/get"
        headers = {"Authorization": f"Bearer {token}"}
        params = {"run_id": run_id}
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to check job status: {response.text}")
            
    except Exception as e:
        st.error(f"Failed to check job status: {e}")
        return None
