import streamlit as st
import pandas as pd
from sklearn.ensemble import IsolationForest
from scipy.stats import zscore

def render(df):
    CANDIDATE_NUMERIC_COLS = ["Failed_Value", "Failed_Row_ID"]
    for col in CANDIDATE_NUMERIC_COLS:
        if col in df.columns:
            df[col + "_num"] = pd.to_numeric(df[col], errors="coerce")

    st.subheader("Anomaly Detection")
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_datetime64_any_dtype(df[col])]
    st.write("Detected numeric columns for anomaly detection:", numeric_cols)

    if not numeric_cols:
        st.error("No numeric columns found for anomaly detection.")
    else:
        selected_cols = st.multiselect("Select numeric columns", numeric_cols, default=numeric_cols)
        method = st.selectbox("Detection Method", ["Z-Score", "Isolation Forest"])
        
        if st.button("Detect Anomalies"):
            df_selected = df[selected_cols].dropna()
            if df_selected.empty:
                st.warning("No complete rows for selected columns after dropping NA.")
            else:
                if method == "Z-Score":
                    z_scores = zscore(df_selected)
                    z_df = pd.DataFrame(z_scores, columns=selected_cols, index=df_selected.index)
                    anomaly_mask = (z_df.abs() > 3).any(axis=1)
                else:
                    model = IsolationForest(contamination=0.05, random_state=42)
                    anomaly_mask = model.fit_predict(df_selected.fillna(0)) == -1
                
                anomalies = df.loc[df_selected.index[anomaly_mask]]
                st.dataframe(anomalies, use_container_width=True)
                st.download_button(
                    "⬇️ Download Anomalies",
                    anomalies.to_csv(index=False),
                    "anomalies.csv"
                )
