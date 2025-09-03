import streamlit as st
import pandas as pd

def render(df):
    st.subheader("Schema Inference")

    schema = pd.DataFrame({
        "Column": df.columns,
        "Data Type": [str(dt) for dt in df.dtypes.values],
        "Nulls": df.isnull().sum().values,
        "Null %": (df.isnull().mean() * 100).round(2),
        "Uniques": df.nunique().values
    })

    st.dataframe(schema, use_container_width=True)
    st.download_button("⬇ Download Schema", schema.to_csv(index=False), "schema.csv")
