import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import os
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH

# 🔧 Databricks Config from environment variables
def get_databricks_connection_params():
    """Get Databricks connection parameters from environment variables"""
    host = os.getenv("DATABRICKS_HOST", DATABRICKS_HOST).replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN", DATABRICKS_TOKEN)
    http_path = os.getenv("DATABRICKS_HTTP_PATH", DATABRICKS_HTTP_PATH)
    
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks connection parameters")
    
    return host, token, http_path

def fetch_table_data(table_name):
    """Fetch data from a specific table in Databricks"""
    try:
        host, token, http_path = get_databricks_connection_params()
        
        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        ) as connection:
            return pd.read_sql(f"SELECT * FROM multitable_logistics.{table_name}", connection)
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

def plot_cleaning_status_summary(df):
    """Plot cleaning status summary"""
    if 'status' in df.columns:
        status_order = ["Fully Cleaned", "Partially Cleaned", "Not Cleaned"]
        color_map = {"Fully Cleaned": "green", "Partially Cleaned": "orange", "Not Cleaned": "gray"}
        counts = df['status'].value_counts()
        status_counts = pd.DataFrame({
            "Status": status_order,
            "Count": [counts.get(s, 0) for s in status_order]
        })
        fig = px.bar(
            status_counts,
            x='Count',
            y='Status',
            orientation='h',
            title="Overall Status",
            color='Status',
            color_discrete_map=color_map
        )
        fig.update_layout(showlegend=False)  
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

def plot_anomaly_count_by_table(df):
    """Plot anomaly count by table"""
    if 'table' in df.columns:
        table_counts = df['table'].value_counts().reset_index()
        table_counts.columns = ['Table', 'Count']
        fig = px.bar(
            table_counts,
            x='Table',
            y='Count',
            title="Anomaly Count by Table",
            color='Count',
            color_continuous_scale='Reds'
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

def plot_validation_status_pie(df):
    """Plot validation status distribution as pie chart"""
    if 'Status' in df.columns:
        status_counts = df['Status'].value_counts()
        
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Validation Status Distribution",
            color_discrete_map={
                'Passed': '#2E8B57',
                'Failed': '#DC143C',
                'Warning': '#FF8C00'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

def plot_rule_failure_frequency(df):
    """Plot rule failure frequency"""
    if 'Rule_Display_Name' in df.columns and 'Status' in df.columns:
        failed_df = df[df['Status'] == 'Failed']
        if not failed_df.empty:
            rule_counts = failed_df['Rule_Display_Name'].value_counts().head(10)
            
            fig = px.bar(
                x=rule_counts.values,
                y=rule_counts.index,
                orientation='h',
                title="Top 10 Most Failed Rules",
                labels={'x': 'Failure Count', 'y': 'Rule Name'},
                color=rule_counts.values,
                color_continuous_scale='Reds'
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No failed validations found.")
        st.markdown("---")

def plot_timeline_trends(df):
    """Plot validation trends over time"""
    if 'Run_Timestamp' in df.columns and 'Status' in df.columns:
        # Convert timestamp and group by date
        df['Run_Date'] = pd.to_datetime(df['Run_Timestamp']).dt.date
        
        # Group by date and status
        daily_counts = df.groupby(['Run_Date', 'Status']).size().reset_index(name='Count')
        
        if not daily_counts.empty:
            fig = px.line(
                daily_counts,
                x='Run_Date',
                y='Count',
                color='Status',
                title="Validation Trends Over Time",
                markers=True,
                color_discrete_map={
                    'Passed': '#2E8B57',
                    'Failed': '#DC143C',
                    'Warning': '#FF8C00'
                }
            )
            fig.update_layout(xaxis_title="Date", yaxis_title="Number of Validations")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No timeline data available.")
        st.markdown("---")

def render(df):
    """Main render function for Data Intelligence Hub"""
    st.subheader("🧠 Data Intelligence Hub")
    st.markdown("Advanced analytics and insights for your data quality metrics.")
    
    if df.empty:
        st.warning("No data available for analysis. Please check your data connection and refresh.")
        return
    
    # Sidebar for additional options
    with st.sidebar:
        st.markdown("### Analysis Options")
        
        analysis_options = st.multiselect(
            "Select Analysis Types",
            ["Status Distribution", "Rule Failures", "Timeline Trends", "Table Analysis"],
            default=["Status Distribution", "Rule Failures"]
        )
        
        # Date range filter
        if 'Run_Timestamp' in df.columns:
            df['Run_Timestamp'] = pd.to_datetime(df['Run_Timestamp'], errors='coerce')
            min_date = df['Run_Timestamp'].min().date()
            max_date = df['Run_Timestamp'].max().date()
            
            date_range = st.date_input(
                "Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                df = df[
                    (df['Run_Timestamp'].dt.date >= start_date) & 
                    (df['Run_Timestamp'].dt.date <= end_date)
                ]
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_validations = len(df)
        st.metric("Total Validations", f"{total_validations:,}")
    
    with col2:
        if 'Status' in df.columns:
            failed_count = len(df[df['Status'] == 'Failed'])
            st.metric("Failed Validations", f"{failed_count:,}")
        else:
            st.metric("Failed Validations", "N/A")
    
    with col3:
        if 'Table' in df.columns:
            unique_tables = df['Table'].nunique()
            st.metric("Tables Monitored", f"{unique_tables:,}")
        else:
            st.metric("Tables Monitored", "N/A")
    
    with col4:
        if 'Rule_Display_Name' in df.columns:
            unique_rules = df['Rule_Display_Name'].nunique()
            st.metric("Active Rules", f"{unique_rules:,}")
        else:
            st.metric("Active Rules", "N/A")
    
    st.markdown("---")
    
    # Main analysis sections
    if "Status Distribution" in analysis_options:
        st.markdown("### 📊 Validation Status Distribution")
        plot_validation_status_pie(df)
    
    if "Rule Failures" in analysis_options:
        st.markdown("### 🚨 Rule Failure Analysis")
        plot_rule_failure_frequency(df)
    
    if "Timeline Trends" in analysis_options:
        st.markdown("### 📈 Validation Trends")
        plot_timeline_trends(df)
    
    if "Table Analysis" in analysis_options:
        st.markdown("### 📋 Table-Level Analysis")
        
        if 'Table' in df.columns and 'Status' in df.columns:
            # Table performance summary
            table_summary = df.groupby('Table')['Status'].agg(['count', lambda x: (x == 'Failed').sum()]).reset_index()
            table_summary.columns = ['Table', 'Total_Validations', 'Failed_Validations']
            table_summary['Success_Rate'] = ((table_summary['Total_Validations'] - table_summary['Failed_Validations']) / table_summary['Total_Validations'] * 100).round(2)
            
            st.markdown("#### Table Performance Summary")
            st.dataframe(
                table_summary.sort_values('Success_Rate'),
                use_container_width=True,
                column_config={
                    "Success_Rate": st.column_config.NumberColumn(
                        "Success Rate (%)",
                        format="%.2f%%"
                    )
                }
            )
        else:
            st.info("Table analysis requires 'Table' and 'Status' columns in the data.")
    
    # Data export section
    st.markdown("---")
    st.markdown("### 📤 Export Analysis Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📥 Export Summary Report"):
            # Create summary report
            summary_data = {
                'Total_Validations': [len(df)],
                'Failed_Validations': [len(df[df['Status'] == 'Failed']) if 'Status' in df.columns else 0],
                'Unique_Tables': [df['Table'].nunique() if 'Table' in df.columns else 0],
                'Unique_Rules': [df['Rule_Display_Name'].nunique() if 'Rule_Display_Name' in df.columns else 0],
                'Analysis_Date': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
            }
            
            summary_df = pd.DataFrame(summary_data)
            csv_summary = summary_df.to_csv(index=False)
            
            st.download_button(
                label="💾 Download Summary CSV",
                data=csv_summary,
                file_name=f"dq_intelligence_summary_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if not df.empty:
            csv_full = df.to_csv(index=False)
            st.download_button(
                label="📊 Export Full Dataset",
                data=csv_full,
                file_name=f"dq_intelligence_full_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    # Advanced insights section
    with st.expander("🔍 Advanced Insights", expanded=False):
        st.markdown("#### Data Quality Insights")
        
        insights = []
        
        if 'Status' in df.columns:
            total_validations = len(df)
            failed_validations = len(df[df['Status'] == 'Failed'])
            success_rate = ((total_validations - failed_validations) / total_validations * 100) if total_validations > 0 else 0
            
            if success_rate >= 95:
                insights.append("✅ Excellent data quality with >95% success rate")
            elif success_rate >= 90:
                insights.append("⚠️ Good data quality but room for improvement")
            else:
                insights.append("🚨 Data quality issues detected - immediate attention required")
        
        if 'Rule_Display_Name' in df.columns and 'Status' in df.columns:
            failed_df = df[df['Status'] == 'Failed']
            if not failed_df.empty:
                most_failed_rule = failed_df['Rule_Display_Name'].value_counts().index[0]
                insights.append(f"🎯 Most problematic rule: {most_failed_rule}")
        
        if 'Table' in df.columns and 'Status' in df.columns:
            table_failure_rates = df.groupby('Table').apply(
                lambda x: (x['Status'] == 'Failed').sum() / len(x) * 100
            ).sort_values(ascending=False)
            
            if not table_failure_rates.empty:
                worst_table = table_failure_rates.index[0]
                worst_rate = table_failure_rates.iloc[0]
                insights.append(f"📋 Table needing attention: {worst_table} ({worst_rate:.1f}% failure rate)")
        
        for insight in insights:
            st.markdown(f"- {insight}")
        
        if not insights:
            st.info("No specific insights available with current data.")
    
    # Connection status
    with st.expander("🔗 Connection Information", expanded=False):
        try:
            host, token, http_path = get_databricks_connection_params()
            st.success("✅ Databricks connection configured")
            st.info(f"Host: {host[:30]}...")
            st.info(f"HTTP Path: {http_path}")
        except Exception as e:
            st.error(f"❌ Connection configuration error: {e}")
            st.warning("Please check your environment variables.")
