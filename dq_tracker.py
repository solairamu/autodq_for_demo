import streamlit as st
import pandas as pd
from config import DQ_STATUS_OPTIONS, DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
from databricks import sql
import os

# ----------------------------------------
# 🔧 Databricks Config from Environment Variables
# ----------------------------------------
def get_databricks_connection_params():
    """Get Databricks connection parameters from environment variables"""
    host = os.getenv("DATABRICKS_HOST", DATABRICKS_HOST).replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN", DATABRICKS_TOKEN)
    http_path = os.getenv("DATABRICKS_HTTP_PATH", DATABRICKS_HTTP_PATH)
    
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks connection parameters")
    
    return host, token, http_path

# ----------------------------------------
# 📌 Required Columns
# ----------------------------------------
REQUIRED_TRACKER_COLS = [
    "Table", "Column", "Rule_Display_Name", "Failed_Row_ID", "Failed_Value",
    "Action_Status", "Assignee", "Notes"
]

def ensure_all_tracker_cols(df):
    """Ensure all required tracker columns exist in the dataframe"""
    for col in REQUIRED_TRACKER_COLS:
        if col not in df.columns:
            df[col] = ""
    return df[REQUIRED_TRACKER_COLS]

# ----------------------------------------
# 📥 Fetch Failed Records (Include User Generated)
# ----------------------------------------
@st.cache_data(ttl=600)
def fetch_failed_records():
    """Fetch failed validation records from Databricks"""
    try:
        host, token, http_path = get_databricks_connection_params()
        
        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        ) as connection:
            query = """
                SELECT 
                    Table, Column, Rule_Display_Name, CAST(Failed_Row_ID AS STRING) as Failed_Row_ID, CAST(Failed_Value AS STRING) as Failed_Value
                FROM multitable_logistics.gx_validation_results_cleaned_combined
                WHERE Status = 'Failed'
                
                UNION
                
                SELECT 
                    Table, Column, Rule_Display_Name, CAST(Failed_Row_ID AS STRING) as Failed_Row_ID, CAST(Failed_Value AS STRING) as Failed_Value
                FROM multitable_logistics.user_defined_validation_log_final_for_dashboard
                WHERE Status = 'Failed'
            """
            df = pd.read_sql(query, connection)
            
            # Additional deduplication at DataFrame level as safeguard
            df = df.drop_duplicates(subset=['Table', 'Column', 'Rule_Display_Name', 'Failed_Row_ID'], keep='first')
            
            return df
    except Exception as e:
        st.error(f"❌ Failed to fetch failed records: {e}")
        return pd.DataFrame()

# ----------------------------------------
# 🚀 Main Renderer
# ----------------------------------------
def render():
    """Main render function for DQ Action Tracker"""
    st.subheader("📋 DQ Action Tracker")
    st.markdown("Track and manage data quality issues, assign responsibilities, and monitor resolution progress.")

    # Initialize session state for action tracker
    if "action_tracker" not in st.session_state:
        st.session_state.action_tracker = pd.DataFrame(columns=REQUIRED_TRACKER_COLS)

    # ----------------------------------------
    # 📊 Summary Metrics
    # ----------------------------------------
    action_df = st.session_state.action_tracker
    
    if not action_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_issues = len(action_df)
            st.metric("Total Issues", total_issues)
        
        with col2:
            open_issues = len(action_df[action_df["Action_Status"].isin(["Open", "In Progress"])])
            st.metric("Open Issues", open_issues)
        
        with col3:
            resolved_issues = len(action_df[action_df["Action_Status"] == "Resolved"])
            st.metric("Resolved Issues", resolved_issues)
        
        with col4:
            if total_issues > 0:
                resolution_rate = (resolved_issues / total_issues) * 100
                st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
            else:
                st.metric("Resolution Rate", "0%")

    st.markdown("---")

    # ----------------------------------------
    # 📥 Load Failed Records Section
    # ----------------------------------------
    st.markdown("### 📥 Import Failed Validations")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("Load failed validation records from Databricks to track and manage.")
    
    with col2:
        if st.button("🔄 Load Failed Records"):
            with st.spinner("Fetching failed records from Databricks..."):
                failed_df = fetch_failed_records()
                
                if not failed_df.empty:
                    # Add default values for tracking columns
                    failed_df["Action_Status"] = "Open"
                    failed_df["Assignee"] = ""
                    failed_df["Notes"] = ""
                    
                    # Ensure all required columns
                    failed_df = ensure_all_tracker_cols(failed_df)
                    
                    # Merge with existing tracker data (avoid duplicates)
                    if not st.session_state.action_tracker.empty:
                        # Create a unique key for deduplication
                        existing_keys = (
                            st.session_state.action_tracker["Table"].astype(str) + "|" +
                            st.session_state.action_tracker["Column"].astype(str) + "|" +
                            st.session_state.action_tracker["Rule_Display_Name"].astype(str) + "|" +
                            st.session_state.action_tracker["Failed_Row_ID"].astype(str)
                        )
                        
                        new_keys = (
                            failed_df["Table"].astype(str) + "|" +
                            failed_df["Column"].astype(str) + "|" +
                            failed_df["Rule_Display_Name"].astype(str) + "|" +
                            failed_df["Failed_Row_ID"].astype(str)
                        )
                        
                        # Only add new records
                        new_records = failed_df[~new_keys.isin(existing_keys)]
                        
                        if not new_records.empty:
                            st.session_state.action_tracker = pd.concat(
                                [st.session_state.action_tracker, new_records], 
                                ignore_index=True
                            )
                            st.success(f"✅ Added {len(new_records)} new failed records to tracker")
                        else:
                            st.info("ℹ️ No new failed records found")
                    else:
                        st.session_state.action_tracker = failed_df
                        st.success(f"✅ Loaded {len(failed_df)} failed records")
                    
                    st.rerun()
                else:
                    st.info("ℹ️ No failed validation records found")

    # ----------------------------------------
    # 📝 Action Tracker Management
    # ----------------------------------------
    st.markdown("### 📝 Action Tracker Management")
    
    if st.session_state.action_tracker.empty:
        st.info("👆 Load failed records above to start tracking data quality issues")
        return

    # ----------------------------------------
    # 🔍 Filters
    # ----------------------------------------
    with st.expander("🔍 Filter Options", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=DQ_STATUS_OPTIONS,
                default=DQ_STATUS_OPTIONS
            )
        
        with col2:
            table_filter = st.multiselect(
                "Filter by Table",
                options=sorted(st.session_state.action_tracker["Table"].dropna().unique()),
                default=sorted(st.session_state.action_tracker["Table"].dropna().unique())
            )
        
        with col3:
            assignee_filter = st.multiselect(
                "Filter by Assignee",
                options=sorted(st.session_state.action_tracker["Assignee"].dropna().unique()),
                default=sorted(st.session_state.action_tracker["Assignee"].dropna().unique())
            )

    # Apply filters
    filtered_df = st.session_state.action_tracker[
        (st.session_state.action_tracker["Action_Status"].isin(status_filter)) &
        (st.session_state.action_tracker["Table"].isin(table_filter)) &
        (st.session_state.action_tracker["Assignee"].isin(assignee_filter) if assignee_filter else True)
    ].copy()

    # ----------------------------------------
    # 📊 Filtered Summary
    # ----------------------------------------
    if not filtered_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Filtered Issues", len(filtered_df))
        
        with col2:
            priority_issues = len(filtered_df[filtered_df["Action_Status"].isin(["Open", "In Progress"])])
            st.metric("Priority Issues", priority_issues)
        
        with col3:
            unique_tables = filtered_df["Table"].nunique()
            st.metric("Affected Tables", unique_tables)

    # ----------------------------------------
    # ✏️ Bulk Actions
    # ----------------------------------------
    st.markdown("#### ⚡ Bulk Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        bulk_status = st.selectbox("Set Status", options=DQ_STATUS_OPTIONS, key="bulk_status")
    
    with col2:
        # Get existing assignees from the tracker
        existing_assignees = sorted([a for a in st.session_state.action_tracker["Assignee"].dropna().unique() if a.strip() != ""])
        assignee_options = [""] + existing_assignees + ["+ Add New Assignee"]
        
        bulk_assignee_selection = st.selectbox("Assign To", options=assignee_options, key="bulk_assignee_selection")
        
        # If user selects "Add New Assignee", show text input
        if bulk_assignee_selection == "+ Add New Assignee":
            bulk_assignee = st.text_input("Enter new assignee name:", key="bulk_assignee_new")
        else:
            bulk_assignee = bulk_assignee_selection
    
    with col3:
        if st.button("📝 Apply to Selected"):
            if not filtered_df.empty:
                # Update the main dataframe
                mask = st.session_state.action_tracker.index.isin(filtered_df.index)
                if bulk_status:
                    st.session_state.action_tracker.loc[mask, "Action_Status"] = bulk_status
                if bulk_assignee:
                    st.session_state.action_tracker.loc[mask, "Assignee"] = bulk_assignee
                
                st.success(f"✅ Updated {len(filtered_df)} records")
                st.rerun()
    
    with col4:
        if st.button("🗑️ Clear Resolved"):
            resolved_mask = st.session_state.action_tracker["Action_Status"] == "Resolved"
            removed_count = resolved_mask.sum()
            st.session_state.action_tracker = st.session_state.action_tracker[~resolved_mask]
            
            if removed_count > 0:
                st.success(f"✅ Removed {removed_count} resolved issues")
                st.rerun()
            else:
                st.info("ℹ️ No resolved issues to remove")

    # ----------------------------------------
    # 📋 Issue Details Table
    # ----------------------------------------
    st.markdown("#### 📋 Issue Details")
    
    if not filtered_df.empty:
        # Create editable dataframe
        edited_df = st.data_editor(
            filtered_df,
            use_container_width=True,
            column_config={
                "Action_Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=DQ_STATUS_OPTIONS,
                    required=True
                ),
                "Assignee": st.column_config.TextColumn(
                    "Assignee",
                    help="Person responsible for resolving this issue"
                ),
                "Notes": st.column_config.TextColumn(
                    "Notes",
                    help="Add notes about resolution progress"
                ),
                "Failed_Row_ID": st.column_config.TextColumn(
                    "Row ID",
                    help="ID of the failed row"
                ),
                "Failed_Value": st.column_config.TextColumn(
                    "Failed Value",
                    help="The value that failed validation"
                )
            },
            disabled=["Table", "Column", "Rule_Display_Name", "Failed_Row_ID", "Failed_Value"],
            hide_index=True,
            key="action_tracker_editor"
        )
        
        # Update session state with edits
        if not edited_df.equals(filtered_df):
            # Update the main dataframe with changes - properly map indices
            for idx in edited_df.index:
                if idx in st.session_state.action_tracker.index:
                    # Update only the editable columns
                    editable_cols = ["Action_Status", "Assignee", "Notes"]
                    for col in editable_cols:
                        if col in edited_df.columns:
                            st.session_state.action_tracker.loc[idx, col] = edited_df.loc[idx, col]
            st.rerun()

    else:
        st.info("ℹ️ No issues match the current filters")

    # ----------------------------------------
    # 📤 Export Options
    # ----------------------------------------
    st.markdown("---")
    st.markdown("### 📤 Export & Reporting")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not st.session_state.action_tracker.empty:
            csv_data = st.session_state.action_tracker.to_csv(index=False)
            st.download_button(
                label="📊 Export All Issues",
                data=csv_data,
                file_name=f"dq_action_tracker_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if not filtered_df.empty:
            filtered_csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="🔍 Export Filtered",
                data=filtered_csv,
                file_name=f"dq_tracker_filtered_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col3:
        if st.button("📊 Generate Summary Report"):
            if not st.session_state.action_tracker.empty:
                # Create summary report
                summary_data = {
                    "Total Issues": [len(st.session_state.action_tracker)],
                    "Open Issues": [len(st.session_state.action_tracker[st.session_state.action_tracker["Action_Status"] == "Open"])],
                    "In Progress": [len(st.session_state.action_tracker[st.session_state.action_tracker["Action_Status"] == "In Progress"])],
                    "Resolved Issues": [len(st.session_state.action_tracker[st.session_state.action_tracker["Action_Status"] == "Resolved"])],
                    "Unique Tables": [st.session_state.action_tracker["Table"].nunique()],
                    "Unique Rules": [st.session_state.action_tracker["Rule_Display_Name"].nunique()],
                    "Report Date": [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_csv = summary_df.to_csv(index=False)
                
                st.download_button(
                    label="💾 Download Summary",
                    data=summary_csv,
                    file_name=f"dq_summary_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

    # ----------------------------------------
    # 🔗 Connection Status
    # ----------------------------------------
    with st.expander("🔗 Connection Information", expanded=False):
        try:
            host, token, http_path = get_databricks_connection_params()
            st.success("✅ Databricks connection configured")
            st.info(f"Host: {host[:30]}...")
            st.info(f"HTTP Path: {http_path}")
        except Exception as e:
            st.error(f"❌ Connection configuration error: {e}")
            st.warning("Please check your environment variables for Databricks connection.")