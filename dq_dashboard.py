import streamlit as st
import pandas as pd
import altair as alt
from config import RULE_CONFIG, DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
from databricks import sql
import os

# 🔧 Databricks Config from environment variables
def get_databricks_connection_params():
    """Get Databricks connection parameters from environment variables"""
    host = os.getenv("DATABRICKS_HOST", DATABRICKS_HOST).replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN", DATABRICKS_TOKEN)
    http_path = os.getenv("DATABRICKS_HTTP_PATH", DATABRICKS_HTTP_PATH)
    
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks connection parameters")
    
    return host, token, http_path

# 📥 Fetch and merge both tables
@st.cache_data(ttl=30)
def fetch_combined_data():
    try:
        host, token, http_path = get_databricks_connection_params()
        
        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        ) as conn:
            df1 = pd.read_sql("SELECT * FROM multitable_logistics.gx_validation_results_cleaned_combined WHERE Metric != 'User Generated Rule'", conn)
            df2 = pd.read_sql("SELECT * FROM multitable_logistics.user_defined_validation_log_final_for_dashboard", conn)
        df = pd.concat([df1, df2], ignore_index=True)
        return df
    except Exception as e:
        st.error(f"Failed to fetch data from Databricks: {e}")
        return pd.DataFrame()

# 🚀 Main Renderer
def render():
    st.subheader("Validation Dashboard")

    df = fetch_combined_data()

    if df.empty:
        st.warning("No data loaded from Databricks. Please check your connection settings.")
        return

    # ✅ Ensure proper datetime for Run_Timestamp
    if "Run_Timestamp" in df.columns:
        df["Run_Timestamp"] = pd.to_datetime(df["Run_Timestamp"], errors="coerce")
        df.dropna(subset=["Run_Timestamp"], inplace=True)

    # ------------------- FILTERS -------------------
    with st.expander("Filter Options", expanded=False):
        available_tables = sorted(df["Table"].dropna().unique())
        available_columns = sorted(df["Column"].dropna().unique())
        available_metrics = sorted(df["Metric"].dropna().unique())
        available_rules = sorted(df["Rule"].dropna().unique())
        available_statuses = sorted(df["Status"].dropna().unique())

        c1, c2 = st.columns(2)
        with c1:
            table_filter = st.multiselect("Select Table(s)", available_tables,default=available_tables)

        with c2:
            column_filter = st.multiselect("Select Column(s)", available_columns, default=available_columns)

        c3, c4 = st.columns(2)
        with c3:
            metric_filter = st.multiselect("Select Metric(s)", available_metrics, default=available_metrics)

        with c4:
            rule_filter = st.multiselect("Select Rule(s)", available_rules, default=available_rules)

        c5, c6 = st.columns(2)
        with c5:
            status_filter = st.multiselect("Select Status(es)", available_statuses, default=available_statuses)

        with c6:
            date_range = st.date_input(
                "Select Date Range",
                value=(df["Run_Timestamp"].min().date() if not df.empty else None, 
                       df["Run_Timestamp"].max().date() if not df.empty else None),
                help="Select the date range for validation results"
            )

    # Apply filters
    filtered_df = df[
        (df["Table"].isin(table_filter)) &
        (df["Column"].isin(column_filter)) &
        (df["Metric"].isin(metric_filter)) &
        (df["Rule"].isin(rule_filter)) &
        (df["Status"].isin(status_filter))
    ]

    # Date range filter
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df["Run_Timestamp"].dt.date >= start_date) &
            (filtered_df["Run_Timestamp"].dt.date <= end_date)
        ]

    if filtered_df.empty:
        st.warning("No data matches the selected filters.")
        return

    # ------------------- METRICS -------------------
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_checks = len(filtered_df)
        st.metric("Total Validation Checks", total_checks)
    
    with col2:
        failed_checks = len(filtered_df[filtered_df["Status"] == "Failed"])
        st.metric("Failed Checks", failed_checks)
    
    with col3:
        success_rate = ((total_checks - failed_checks) / total_checks * 100) if total_checks > 0 else 0
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    with col4:
        unique_tables = filtered_df["Table"].nunique()
        st.metric("Tables Monitored", unique_tables)

    # ------------------- VISUALIZATIONS -------------------
    
    # Status Distribution
    st.subheader("📊 Validation Status Distribution")
    status_counts = filtered_df["Status"].value_counts()
    
    if not status_counts.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Pie chart for status distribution
            chart = alt.Chart(status_counts.reset_index()).mark_arc().encode(
                theta=alt.Theta(field="count", type="quantitative"),
                color=alt.Color(field="Status", type="nominal", 
                               scale=alt.Scale(domain=["Passed", "Failed"], 
                                             range=["#2E8B57", "#DC143C"])),
                tooltip=["Status", "count"]
            ).properties(
                width=300,
                height=300
            )
            st.altair_chart(chart, use_container_width=True)
        
        with col2:
            st.dataframe(status_counts, use_container_width=True)

    # Validation Trends Over Time
    if "Run_Timestamp" in filtered_df.columns:
        st.subheader("📈 Validation Trends Over Time")
        
        # Group by date and status
        daily_trends = filtered_df.groupby([
            filtered_df["Run_Timestamp"].dt.date,
            "Status"
        ]).size().reset_index(name="count")
        daily_trends.columns = ["date", "Status", "count"]
        
        # Convert date column to datetime to avoid Altair dtype issues
        daily_trends["date"] = pd.to_datetime(daily_trends["date"])
        
        if not daily_trends.empty:
            trend_chart = alt.Chart(daily_trends).mark_line(point=True).encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("count:Q", title="Number of Checks"),
                color=alt.Color("Status:N", 
                               scale=alt.Scale(domain=["Passed", "Failed"], 
                                             range=["#2E8B57", "#DC143C"])),
                tooltip=["date", "Status", "count"]
            ).properties(
                width=600,
                height=300
            )
            st.altair_chart(trend_chart, use_container_width=True)

    # Failed Checks by Table
    if failed_checks > 0:
        st.subheader("🚨 Failed Checks by Table")
        failed_by_table = filtered_df[filtered_df["Status"] == "Failed"]["Table"].value_counts().head(10)
        
        if not failed_by_table.empty:
            failed_chart = alt.Chart(failed_by_table.reset_index()).mark_bar().encode(
                x=alt.X("count:Q", title="Number of Failed Checks"),
                y=alt.Y("Table:N", sort="-x", title="Table"),
                color=alt.value("#DC143C"),
                tooltip=["Table", "count"]
            ).properties(
                width=600,
                height=300
            )
            st.altair_chart(failed_chart, use_container_width=True)

    # ------------------- DETAILED VIEW -------------------
    st.subheader("🔍 Detailed Validation Results")
    
    # Show key columns for the detailed view
    display_columns = ["Run_Timestamp", "Table", "Column", "Rule", "Status", "Metric"]
    available_display_columns = [col for col in display_columns if col in filtered_df.columns]
    
    if available_display_columns:
        st.dataframe(
            filtered_df[available_display_columns].sort_values("Run_Timestamp", ascending=False),
            use_container_width=True,
            height=400
        )
    else:
        st.dataframe(filtered_df, use_container_width=True, height=400)

    # ------------------- EXPORT OPTIONS -------------------
    if not filtered_df.empty:
        st.subheader("📤 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"validation_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            if failed_checks > 0:
                failed_csv = filtered_df[filtered_df["Status"] == "Failed"].to_csv(index=False)
                st.download_button(
                    label="Download Failed Checks Only",
                    data=failed_csv,
                    file_name=f"failed_validations_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
