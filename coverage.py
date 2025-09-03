import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

def render(df):
    st.subheader("Check Coverage Overview")
    st.markdown("This heatmap shows which tables and columns have active data quality checks and which do not. Areas with no checks are 'blind spots'.")

    coverage = (
        df.groupby(['Table', 'Column'])['Rule_Display_Name']
        .nunique().unstack(fill_value=0).reset_index()
    )

    gb = GridOptionsBuilder.from_dataframe(coverage)
    cell_style_jscode = JsCode("""
    function(params) {
        if (params.value == 0) {
            return { 'backgroundColor': '#f8d7da', 'color': 'black' } // Soft Red
        } else if (params.value <= 10) {
            return { 'backgroundColor': '#fff3cd', 'color': 'black' } // Soft Yellow
        } else {
            return { 'backgroundColor': '#d4edda', 'color': 'black' } // Soft Green
        }
    };
    """)

    for col in coverage.columns[1:]:
        gb.configure_column(col, cellStyle=cell_style_jscode, tooltipField=col)

    gb.configure_column("Table", pinned="left", headerName="Table Name", width=180)
    gb.configure_default_column(resizable=True, sortable=True, filter=True)
    gb.configure_grid_options(domLayout='autoHeight')
    gb.configure_side_bar()
    gridOptions = gb.build()

    AgGrid(
        coverage,
        gridOptions=gridOptions,
        enable_enterprise_modules=False,
        allow_unsafe_jscode=True,
        theme="balham",
        fit_columns_on_grid_load=False,
        height=500
    )
