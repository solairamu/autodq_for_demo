import streamlit as st

def render(df):
    st.subheader("Alert Feed & Incident Summary")
    st.markdown("Lists failed checks and incidents detected from Databricks validation results.")

    failed_records = df[df['Status'] == 'Failed'].copy()

    if not failed_records.empty:
        alert_feed = failed_records[['Run_Timestamp', 'Table', 'Rule_Display_Name', 'Status', 'Failure_Type']].copy()
        alert_feed.rename(columns={
            'Run_Timestamp': 'Time',
            'Table': 'Table',
            'Rule_Display_Name': 'Rule',
            'Status': 'Status',
            'Failure_Type': 'Message'
        }, inplace=True)

        critical_rules = ["No Nulls", "Unique Values", "Primary Key Present", "Foreign Key Valid"]
        alert_feed['Severity'] = alert_feed['Rule'].apply(lambda x: 'Critical' if x in critical_rules else 'Warning')

        alert_feed.sort_values(by='Time', ascending=False, inplace=True)
        st.dataframe(alert_feed, use_container_width=True)
        st.success(f"Displaying {len(alert_feed)} failed checks from live validation results.")
    else:
        st.info("No failed checks currently detected from Databricks validation results.")

    st.info("Integrate with Slack/email APIs for production notifications.")
