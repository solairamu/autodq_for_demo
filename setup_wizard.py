# setup_wizard.py

import os
import sys
import json
import streamlit as st
from pathlib import Path
from databricks import sql
import requests
from urllib.parse import urlparse

class DatabricksSetupWizard:
    """Interactive setup wizard for configuring Databricks connection for clients"""
    
    def __init__(self):
        self.env_file_path = Path(".env")
        self.config_file_path = Path("client_config.json")
        
    def run_setup_wizard(self):
        """Main setup wizard interface"""
        st.title("🚀 AutoDQ Setup Wizard")
        st.markdown("Welcome! Let's configure AutoDQ to connect to your Databricks workspace.")
        
        # Check if already configured
        if self.env_file_path.exists():
            if st.button("🔄 Reconfigure Connection"):
                self._clear_existing_config()
            else:
                st.success("✅ Configuration already exists!")
                if st.button("🧪 Test Current Connection"):
                    self._test_connection()
                return
        
        # Step-by-step configuration
        self._display_setup_steps()
        
    def _display_setup_steps(self):
        """Display the setup steps"""
        
        # Step 1: Deployment Type Detection
        st.header("Step 1: Deployment Detection")
        deployment_type = self._detect_deployment_type()
        
        if deployment_type == "databricks_runtime":
            st.success("🎉 Running in Databricks! No additional configuration needed.")
            self._configure_for_databricks_runtime()
            return
        elif deployment_type == "local_or_client":
            st.info("🖥️ Running locally or on client infrastructure. Manual configuration required.")
            
        # Step 2: Databricks Connection
        st.header("Step 2: Databricks Connection")
        self._configure_databricks_connection()
        
    def _detect_deployment_type(self):
        """Detect if running in Databricks environment or locally"""
        databricks_indicators = [
            os.getenv("DATABRICKS_RUNTIME_VERSION"),
            os.getenv("DATABRICKS_WORKSPACE_URL"),
            os.path.exists("/databricks/driver")
        ]
        
        if any(databricks_indicators):
            st.info("🔍 **Detected**: Databricks Runtime Environment")
            return "databricks_runtime"
        else:
            st.info("🔍 **Detected**: Local/Client Environment")
            return "local_or_client"
    
    def _configure_for_databricks_runtime(self):
        """Configure for Databricks runtime environment"""
        config = {
            "deployment_type": "databricks_runtime",
            "configured_at": str(pd.Timestamp.now()),
            "auto_configured": True
        }
        
        # Save minimal config
        with open(self.config_file_path, 'w') as f:
            json.dump(config, f, indent=2)
            
        st.success("✅ Configuration complete! AutoDQ will use automatic Databricks authentication.")
        
    def _configure_databricks_connection(self):
        """Interactive Databricks connection configuration"""
        
        st.markdown("""
        ### 📋 You'll need the following information from your Databricks workspace:
        
        1. **Workspace URL** - Your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
        2. **Access Token** - Personal access token or service principal token
        3. **SQL Warehouse HTTP Path** - Path to your SQL warehouse
        4. **Default Schema** - Schema containing your data quality tables
        """)
        
        # Get workspace URL
        workspace_url = st.text_input(
            "🌐 Databricks Workspace URL",
            placeholder="https://your-workspace.cloud.databricks.com",
            help="Copy this from your browser when logged into Databricks"
        )
        
        # Get access token
        access_token = st.text_input(
            "🔑 Access Token", 
            type="password",
            help="Generate from: User Settings → Developer → Access Tokens"
        )
        
        # Get HTTP path
        http_path = st.text_input(
            "🔗 SQL Warehouse HTTP Path",
            placeholder="/sql/1.0/warehouses/your-warehouse-id",
            help="Find in: SQL Warehouses → Your Warehouse → Connection Details"
        )
        
        # Get default schema
        default_schema = st.text_input(
            "📊 Default Schema",
            value="multitable_logistics",
            help="Schema containing your data quality validation tables"
        )
        
        # Additional settings
        st.subheader("⚙️ Additional Settings")
        
        refresh_interval = st.number_input(
            "🔄 Auto-refresh Interval (minutes)",
            min_value=1,
            max_value=60,
            value=10,
            help="How often to refresh data from Databricks"
        )
        
        # Test and save configuration
        if st.button("🧪 Test Connection & Save Configuration"):
            if self._validate_and_save_config(workspace_url, access_token, http_path, default_schema, refresh_interval):
                st.success("✅ Configuration saved successfully!")
                st.balloons()
            else:
                st.error("❌ Configuration failed. Please check your settings and try again.")
    
    def _validate_and_save_config(self, workspace_url, access_token, http_path, default_schema, refresh_interval):
        """Validate connection and save configuration"""
        
        # Validate inputs
        if not all([workspace_url, access_token, http_path, default_schema]):
            st.error("Please fill in all required fields.")
            return False
            
        # Normalize workspace URL
        if not workspace_url.startswith('https://'):
            workspace_url = f"https://{workspace_url}"
            
        try:
            # Test connection
            with st.spinner("Testing Databricks connection..."):
                server_hostname = workspace_url.replace("https://", "")
                
                with sql.connect(
                    server_hostname=server_hostname,
                    http_path=http_path,
                    access_token=access_token
                ) as connection:
                    # Test basic connectivity
                    cursor = connection.cursor()
                    cursor.execute("SELECT 1 as test")
                    result = cursor.fetchone()
                    
                    if result[0] != 1:
                        raise Exception("Connection test failed")
                        
                    # Test schema access
                    try:
                        cursor.execute(f"SHOW TABLES IN {default_schema}")
                        tables = cursor.fetchall()
                        st.info(f"✅ Found {len(tables)} tables in schema '{default_schema}'")
                    except Exception as e:
                        st.warning(f"⚠️ Could not access schema '{default_schema}': {e}")
                        st.info("The schema will be created automatically when data is first loaded.")
            
            # Save configuration to .env file
            from datetime import datetime
            env_content = f"""# AutoDQ for Databricks - Client Configuration
# Generated by Setup Wizard on {datetime.now()}

# REQUIRED DATABRICKS CONNECTION SETTINGS
DATABRICKS_HOST={workspace_url}
DATABRICKS_TOKEN={access_token}
DATABRICKS_HTTP_PATH={http_path}

# APPLICATION SETTINGS
DEFAULT_SCHEMA={default_schema}
DEFAULT_REFRESH_INTERVAL={refresh_interval}

# SECURITY SETTINGS
DATABRICKS_SSL_VERIFY=true

# APPLICATION PORT
DATABRICKS_APP_PORT=8501
"""
            
            with open(self.env_file_path, 'w') as f:
                f.write(env_content)
                
            # Save client configuration metadata
            client_config = {
                "deployment_type": "client_configured",
                "configured_at": str(datetime.now()),
                "workspace_url": workspace_url,
                "default_schema": default_schema,
                "refresh_interval": refresh_interval,
                "setup_completed": True
            }
            
            with open(self.config_file_path, 'w') as f:
                json.dump(client_config, f, indent=2)
                
            return True
            
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            self._display_troubleshooting_help(e)
            return False
    
    def _test_connection(self):
        """Test existing connection configuration"""
        try:
            from config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
            
            if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH]):
                st.error("❌ Missing configuration. Please reconfigure.")
                return
                
            with st.spinner("Testing connection..."):
                server_hostname = DATABRICKS_HOST.replace("https://", "")
                
                with sql.connect(
                    server_hostname=server_hostname,
                    http_path=DATABRICKS_HTTP_PATH,
                    access_token=DATABRICKS_TOKEN
                ) as connection:
                    cursor = connection.cursor()
                    cursor.execute("SELECT current_timestamp() as test_time")
                    result = cursor.fetchone()
                    
                    st.success(f"✅ Connection successful! Server time: {result[0]}")
                    
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")
            self._display_troubleshooting_help(e)
    
    def _display_troubleshooting_help(self, error):
        """Display troubleshooting help based on error type"""
        
        error_str = str(error).lower()
        
        st.subheader("🔧 Troubleshooting Help")
        
        if "authentication" in error_str or "token" in error_str:
            st.markdown("""
            **Authentication Issues:**
            - Check if your access token is correct and not expired
            - Generate a new token: User Settings → Developer → Access Tokens
            - Ensure the token has sufficient permissions
            """)
            
        elif "host" in error_str or "connection" in error_str:
            st.markdown("""
            **Connection Issues:**
            - Verify your workspace URL is correct
            - Ensure your SQL warehouse is running
            - Check if you're behind a corporate firewall
            """)
            
        elif "warehouse" in error_str or "http_path" in error_str:
            st.markdown("""
            **SQL Warehouse Issues:**
            - Verify the HTTP path is correct
            - Ensure the SQL warehouse is running
            - Check warehouse permissions
            """)
            
        else:
            st.markdown("""
            **General Troubleshooting:**
            - Double-check all connection parameters
            - Ensure SQL warehouse is running
            - Verify network connectivity to Databricks
            - Check token permissions and expiration
            """)
    
    def _clear_existing_config(self):
        """Clear existing configuration files"""
        try:
            if self.env_file_path.exists():
                self.env_file_path.unlink()
            if self.config_file_path.exists():
                self.config_file_path.unlink()
            st.info("🧹 Existing configuration cleared.")
        except Exception as e:
            st.error(f"Error clearing configuration: {e}")

def main():
    """Main function for running setup wizard standalone"""
    st.set_page_config(
        page_title="AutoDQ Setup Wizard",
        page_icon="🚀",
        layout="wide"
    )
    
    wizard = DatabricksSetupWizard()
    wizard.run_setup_wizard()

if __name__ == "__main__":
    main()
