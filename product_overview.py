import streamlit as st
import base64

@st.cache_data
def get_architecture_diagram(path):
    """Load and encode architecture diagram with caching"""
    try:
        with open(path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode()
    except FileNotFoundError:
        st.warning(f"Architecture diagram not found: {path}")
        return None

def render():
    st.title("About AutoDQ")

    st.markdown("""
    **AutoDQ** is a comprehensive Data Quality and Validation platform designed to:

    - **Detect data quality issues** across large datasets.
    - **Provide visual insights** into validation coverage and blind spots.
    - **Enable data cleaning, anomaly detection, and schema inference** seamlessly.
    - **Track data quality actions and resolutions** via the DQ Action Tracker.
    - **Integrate with Databricks and cloud platforms** for scalable validation.

    Whether you're migrating data or monitoring production pipelines, AutoDQ ensures your data is **trustworthy, complete, and reliable.**
    """)

    st.markdown("---")
    st.header("Architecture")

    # Load architecture diagram from images folder (cached)
    image_path = "./images/Background.png"
    encoded_image = get_architecture_diagram(image_path)
    
    if encoded_image:
        st.markdown(
            f"""
            <div style="text-align: center;">
                <img src="data:image/png;base64,{encoded_image}" width="1100">
                <p><em>AutoDQ Architecture Diagram</em></p>
            </div>
            """,
            unsafe_allow_html=True
        )
        
    else:
        st.info("📊 Architecture diagram could not be loaded. Using text-based overview instead.")
    
    st.markdown("""
    **AutoDQ Architecture Overview:**
    
    1. **Data Sources** → Various data sources feeding into Databricks
    2. **Databricks Platform** → Central data platform with Unity Catalog governance
    3. **AutoDQ Engine** → Data quality validation and monitoring
    4. **Interactive Dashboard** → Real-time insights and alerting
    5. **Action Tracking** → Issue resolution and workflow management
    """)