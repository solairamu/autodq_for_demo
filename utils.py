# utils.py

import json
import pandas as pd
import streamlit as st

def to_json(obj, path):
    """Save a Python dict as a JSON file."""
    with open(path, 'w') as f:
        json.dump(obj, f, indent=4)

def from_json(path):
    """Load a Python dict from a JSON file."""
    with open(path, 'r') as f:
        return json.load(f)

def dataframe_to_csv_download(df: pd.DataFrame, file_name: str, label: str = "⬇ Download CSV"):
    st.download_button(label, df.to_csv(index=False), file_name)

def format_metric(value, default="N/A"):
    if value is None or pd.isna(value):
        return default
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)

def get_unique_list(series):
    return sorted(series.dropna().unique().tolist())

def error_message_box(msg, details=None):
    st.error(msg)
    if details:
        st.caption(details)

def info_message_box(msg, details=None):
    st.info(msg)
    if details:
        st.caption(details)

def success_message_box(msg, details=None):
    st.success(msg)
    if details:
        st.caption(details)

def list_columns_by_dtype(df: pd.DataFrame, dtype="object"):
    return [col for col in df.columns if df[col].dtype == dtype]

def list_numeric_columns(df: pd.DataFrame):
    return [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]

def list_datetime_columns(df: pd.DataFrame):
    return [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
