import streamlit as st
import pandas as pd

def render(df):
    st.title("Smart Data Cleaning & Change Tracking")

    cleaned_df = df.copy()
    changes_log = []

    st.markdown("## Step 1: Apply Cleaning Options")

    col1, col2, col3 = st.columns(3)

    with col1:
        apply_nulls = st.checkbox("Remove Null Rows")
    with col2:
        apply_lowercase = st.checkbox("Lowercase Text Columns")
    with col3:
        apply_duplicates = st.checkbox("Remove Duplicate Rows")

    # --- Null Removal ---
    if apply_nulls:
        rows_before = len(cleaned_df)
        cleaned_df = cleaned_df.dropna()
        rows_after = len(cleaned_df)
        removed = rows_before - rows_after

        if removed > 0:
            changes_log.append(["All Tables", "All Columns", "Null Rows Removed", removed])

        st.markdown("### ✅ Data After Null Removal")
        st.dataframe(cleaned_df.head(), use_container_width=True)

    # --- Lowercase ---
    if apply_lowercase:
        text_cols = cleaned_df.select_dtypes(include='object').columns
        changed_cols = []
        for col in text_cols:
            before = cleaned_df[col].copy()
            cleaned_df[col] = cleaned_df[col].str.lower()
            if not before.equals(cleaned_df[col]):
                changed_cols.append(col)

        for col in changed_cols:
            changes_log.append(["All Tables", col, "Lowercased", "Yes"])

        if changed_cols:
            st.markdown("### ✅ Data After Lowercasing Text Columns")
            st.dataframe(cleaned_df[changed_cols].head(), use_container_width=True)
        else:
            st.info("No text columns available for lowercasing.")

    # --- Duplicates ---
    if apply_duplicates:
        rows_before = len(cleaned_df)
        cleaned_df = cleaned_df.drop_duplicates()
        rows_after = len(cleaned_df)
        removed = rows_before - rows_after

        if removed > 0:
            changes_log.append(["All Tables", "All Columns", "Duplicates Removed", removed])

        st.markdown("### ✅ Data After Removing Duplicates")
        st.dataframe(cleaned_df, use_container_width=True)

    st.markdown("---")
    st.markdown("## Step 2: Changes Summary")

    if changes_log:
        changes_df = pd.DataFrame(changes_log, columns=["Table", "Column", "Change Applied", "Details"])
        st.dataframe(changes_df, use_container_width=True)
    else:
        st.info("No changes applied yet.")

    st.markdown("---")
    st.markdown("## Step 3: Visual Insights (Optional)")

    numeric_cols = cleaned_df.select_dtypes(include='number').columns
    if len(numeric_cols) > 0:
        selected_num = st.selectbox("Select a Numeric Column to Visualize Distribution:", numeric_cols)
        st.line_chart(cleaned_df[selected_num])
    else:
        st.info("No numeric columns available for visualization.")

    st.markdown("---")
    st.download_button("⬇️ Download Cleaned Data", cleaned_df.to_csv(index=False), "cleaned_data.csv")
