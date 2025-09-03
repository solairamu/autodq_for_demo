# environment_detector.py

import os
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import logging

class EnvironmentDetector:
    """Detects and manages different deployment environments for AutoDQ"""
    
    def __init__(self):
        self.config_file = Path("client_config.json")
        self.env_file = Path(".env")
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging for environment detection"""
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def detect_environment(self) -> Dict[str, Any]:
        """
        Detect the current deployment environment and return configuration
        
        Returns:
            Dict containing environment type and configuration details
        """
        
        # Check for Databricks runtime environment
        if self._is_databricks_runtime():
            return self._get_databricks_runtime_config()
            
        # Check for Databricks Lakehouse Apps
        if self._is_databricks_lakehouse_app():
            return self._get_lakehouse_app_config()
            
        # Check for client/local configuration
        if self._has_client_config():
            return self._get_client_config()
            
        # Default to unconfigured local environment
        return self._get_unconfigured_config()
    
    def _is_databricks_runtime(self) -> bool:
        """Check if running in Databricks runtime environment"""
        indicators = [
            os.getenv("DATABRICKS_RUNTIME_VERSION"),
            os.getenv("DATABRICKS_WORKSPACE_URL"), 
            os.getenv("DATABRICKS_WORKSPACE_ID"),
            os.path.exists("/databricks/driver"),
            os.path.exists("/databricks/spark")
        ]
        
        is_databricks = any(indicators)
        if is_databricks:
            self.logger.info("Detected Databricks runtime environment")
        
        return is_databricks
    
    def _is_databricks_lakehouse_app(self) -> bool:
        """Check if running as a Databricks Lakehouse App"""
        lakehouse_indicators = [
            os.getenv("DATABRICKS_APP_ID"),
            os.getenv("DATABRICKS_APP_NAME"),
            os.getenv("LAKEHOUSE_APP_MODE")
        ]
        
        is_lakehouse = any(lakehouse_indicators)
        if is_lakehouse:
            self.logger.info("Detected Databricks Lakehouse App environment")
            
        return is_lakehouse
    
    def _has_client_config(self) -> bool:
        """Check if client configuration exists"""
        return self.env_file.exists() or self.config_file.exists()
    
    def _get_databricks_runtime_config(self) -> Dict[str, Any]:
        """Get configuration for Databricks runtime environment"""
        return {
            "environment_type": "databricks_runtime",
            "requires_setup": False,
            "auto_authentication": True,
            "connection_method": "databricks_sql_connect",
            "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
            "workspace_id": os.getenv("DATABRICKS_WORKSPACE_ID"),
            "runtime_version": os.getenv("DATABRICKS_RUNTIME_VERSION"),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
            "default_schema": os.getenv("DEFAULT_SCHEMA", "multitable_logistics"),
            "detected_features": self._get_databricks_features()
        }
    
    def _get_lakehouse_app_config(self) -> Dict[str, Any]:
        """Get configuration for Databricks Lakehouse App"""
        return {
            "environment_type": "databricks_lakehouse_app",
            "requires_setup": False,
            "auto_authentication": True,
            "connection_method": "lakehouse_app_connect",
            "app_id": os.getenv("DATABRICKS_APP_ID"),
            "app_name": os.getenv("DATABRICKS_APP_NAME"),
            "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
            "default_schema": os.getenv("DEFAULT_SCHEMA", "multitable_logistics"),
            "app_port": os.getenv("DATABRICKS_APP_PORT", "8501")
        }
    
    def _get_client_config(self) -> Dict[str, Any]:
        """Get configuration for client/local environment"""
        
        # Load client configuration if exists
        client_metadata = {}
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    client_metadata = json.load(f)
            except Exception as e:
                self.logger.warning(f"Could not load client config: {e}")
        
        # Check environment variables
        env_config = self._load_env_config()
        
        return {
            "environment_type": "client_configured",
            "requires_setup": not self._is_fully_configured(env_config),
            "auto_authentication": False,
            "connection_method": "manual_credentials",
            "databricks_host": env_config.get("DATABRICKS_HOST"),
            "databricks_token": "***" + env_config.get("DATABRICKS_TOKEN", "")[-4:] if env_config.get("DATABRICKS_TOKEN") else None,
            "databricks_http_path": env_config.get("DATABRICKS_HTTP_PATH"),
            "default_schema": env_config.get("DEFAULT_SCHEMA", "multitable_logistics"),
            "refresh_interval": env_config.get("DEFAULT_REFRESH_INTERVAL", "10"),
            "client_metadata": client_metadata,
            "config_files_present": {
                "env_file": self.env_file.exists(),
                "config_file": self.config_file.exists()
            }
        }
    
    def _get_unconfigured_config(self) -> Dict[str, Any]:
        """Get configuration for unconfigured environment"""
        return {
            "environment_type": "unconfigured",
            "requires_setup": True,
            "auto_authentication": False,
            "connection_method": "needs_configuration",
            "setup_required": True,
            "available_setup_methods": [
                "interactive_wizard",
                "environment_file", 
                "configuration_script"
            ]
        }
    
    def _load_env_config(self) -> Dict[str, str]:
        """Load configuration from environment variables"""
        env_vars = [
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN", 
            "DATABRICKS_HTTP_PATH",
            "DEFAULT_SCHEMA",
            "DEFAULT_REFRESH_INTERVAL",
            "DATABRICKS_SSL_VERIFY"
        ]
        
        return {var: os.getenv(var, "") for var in env_vars}
    
    def _is_fully_configured(self, env_config: Dict[str, str]) -> bool:
        """Check if environment is fully configured"""
        required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]
        return all(env_config.get(var) for var in required_vars)
    
    def _get_databricks_features(self) -> Dict[str, bool]:
        """Detect available Databricks features"""
        return {
            "unity_catalog": self._has_unity_catalog(),
            "sql_warehouses": self._has_sql_warehouses(),
            "jobs_api": self._has_jobs_api(),
            "secrets_scope": self._has_secrets_scope()
        }
    
    def _has_unity_catalog(self) -> bool:
        """Check if Unity Catalog is available"""
        try:
            # Check for Unity Catalog environment variables
            return bool(os.getenv("DATABRICKS_CATALOG") or os.getenv("UNITY_CATALOG_ENABLED"))
        except:
            return False
    
    def _has_sql_warehouses(self) -> bool:
        """Check if SQL Warehouses are available"""
        # Most Databricks workspaces have SQL warehouses
        return True
    
    def _has_jobs_api(self) -> bool:
        """Check if Jobs API is available"""
        return bool(os.getenv("DATABRICKS_JOB_ID"))
    
    def _has_secrets_scope(self) -> bool:
        """Check if Databricks Secrets are available"""
        try:
            # Check for secrets-related environment variables
            return bool(os.getenv("DATABRICKS_SECRETS_SCOPE"))
        except:
            return False
    
    def get_connection_config(self) -> Dict[str, Any]:
        """Get connection configuration for the current environment"""
        env_config = self.detect_environment()
        
        if env_config["environment_type"] in ["databricks_runtime", "databricks_lakehouse_app"]:
            return {
                "use_automatic_auth": True,
                "connection_params": {}
            }
        elif env_config["environment_type"] == "client_configured":
            env_vars = self._load_env_config()
            return {
                "use_automatic_auth": False,
                "connection_params": {
                    "server_hostname": env_vars.get("DATABRICKS_HOST", "").replace("https://", ""),
                    "http_path": env_vars.get("DATABRICKS_HTTP_PATH", ""),
                    "access_token": env_vars.get("DATABRICKS_TOKEN", ""),
                    "ssl_verify": env_vars.get("DATABRICKS_SSL_VERIFY", "false").lower() == "true"
                }
            }
        else:
            return {
                "use_automatic_auth": False,
                "connection_params": {},
                "requires_setup": True
            }
    
    def save_client_config(self, config: Dict[str, Any]) -> bool:
        """Save client configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save client config: {e}")
            return False
    
    def get_setup_instructions(self) -> Dict[str, Any]:
        """Get setup instructions based on current environment"""
        env_config = self.detect_environment()
        
        if env_config["requires_setup"]:
            return {
                "setup_required": True,
                "recommended_method": "interactive_wizard",
                "instructions": {
                    "interactive_wizard": "Run the setup wizard: streamlit run setup_wizard.py",
                    "manual_config": "Create .env file with Databricks credentials",
                    "environment_vars": "Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH"
                },
                "next_steps": [
                    "Gather Databricks workspace URL",
                    "Generate access token",
                    "Find SQL warehouse HTTP path",
                    "Run setup wizard or create .env file"
                ]
            }
        else:
            return {
                "setup_required": False,
                "status": "Ready to use",
                "environment_type": env_config["environment_type"]
            }

# Singleton instance for global use
environment_detector = EnvironmentDetector()
