#!/usr/bin/env python3
# deploy_for_client.py

"""
AutoDQ Client Deployment Script

This script helps clients set up AutoDQ for their Databricks environment.
It can be run interactively or with command-line arguments.

Usage:
    python deploy_for_client.py                    # Interactive mode
    python deploy_for_client.py --wizard           # Run setup wizard
    python deploy_for_client.py --validate         # Validate existing config
    python deploy_for_client.py --install          # Install dependencies only
"""

import os
import sys
import argparse
import subprocess
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging

class AutoDQClientDeployer:
    """Handles AutoDQ deployment for client environments"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.env_file = self.project_root / ".env"
        self.config_file = self.project_root / "client_config.json"
        self.requirements_file = self.project_root / "requirements.txt"
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging for deployment"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def deploy(self, mode: str = "interactive") -> bool:
        """
        Main deployment method
        
        Args:
            mode: Deployment mode (interactive, wizard, validate, install)
            
        Returns:
            bool: True if deployment successful
        """
        
        self.logger.info("🚀 Starting AutoDQ Client Deployment")
        
        try:
            if mode == "install":
                return self._install_dependencies()
            elif mode == "validate":
                return self._validate_configuration()
            elif mode == "wizard":
                return self._run_setup_wizard()
            else:  # interactive
                return self._interactive_deployment()
                
        except KeyboardInterrupt:
            self.logger.info("\n❌ Deployment cancelled by user")
            return False
        except Exception as e:
            self.logger.error(f"❌ Deployment failed: {e}")
            return False
    
    def _interactive_deployment(self) -> bool:
        """Interactive deployment with menu options"""
        
        print("\n" + "="*60)
        print("🎯 AutoDQ Client Deployment")
        print("="*60)
        
        # Check current status
        self._display_current_status()
        
        while True:
            print("\nWhat would you like to do?")
            print("1. 📦 Install dependencies")
            print("2. ⚙️  Run setup wizard")
            print("3. 🧪 Validate configuration")
            print("4. 🚀 Start AutoDQ")
            print("5. 📋 View configuration")
            print("6. 🔄 Reset configuration")
            print("0. ❌ Exit")
            
            choice = input("\nEnter your choice (0-6): ").strip()
            
            if choice == "0":
                print("👋 Goodbye!")
                return True
            elif choice == "1":
                self._install_dependencies()
            elif choice == "2":
                self._run_setup_wizard()
            elif choice == "3":
                self._validate_configuration()
            elif choice == "4":
                self._start_autodq()
            elif choice == "5":
                self._view_configuration()
            elif choice == "6":
                self._reset_configuration()
            else:
                print("❌ Invalid choice. Please try again.")
    
    def _display_current_status(self):
        """Display current deployment status"""
        
        print("\n📊 Current Status:")
        
        # Check Python version
        python_version = sys.version_info
        if python_version >= (3, 8):
            print(f"✅ Python {python_version.major}.{python_version.minor}.{python_version.micro}")
        else:
            print(f"⚠️  Python {python_version.major}.{python_version.minor}.{python_version.micro} (3.8+ recommended)")
        
        # Check dependencies
        if self._check_dependencies():
            print("✅ Dependencies installed")
        else:
            print("❌ Dependencies missing")
        
        # Check configuration
        if self.env_file.exists():
            print("✅ Configuration file exists")
            if self._is_configuration_valid():
                print("✅ Configuration appears valid")
            else:
                print("⚠️  Configuration may be incomplete")
        else:
            print("❌ Configuration file missing")
        
        # Check Databricks connectivity
        if self._can_connect_to_databricks():
            print("✅ Databricks connection working")
        else:
            print("❌ Cannot connect to Databricks")
    
    def _install_dependencies(self) -> bool:
        """Install required dependencies"""
        
        self.logger.info("📦 Installing dependencies...")
        
        if not self.requirements_file.exists():
            self.logger.error("❌ requirements.txt not found")
            return False
        
        try:
            # Install main requirements
            subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", str(self.requirements_file)
            ], check=True)
            
            # Install additional client dependencies
            client_deps = ["python-dotenv", "streamlit"]
            subprocess.run([
                sys.executable, "-m", "pip", "install"
            ] + client_deps, check=True)
            
            self.logger.info("✅ Dependencies installed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Failed to install dependencies: {e}")
            return False
    
    def _run_setup_wizard(self) -> bool:
        """Run the interactive setup wizard"""
        
        self.logger.info("🧙 Starting setup wizard...")
        
        wizard_file = self.project_root / "setup_wizard.py"
        if not wizard_file.exists():
            self.logger.error("❌ Setup wizard not found")
            return False
        
        try:
            subprocess.run([
                sys.executable, "-m", "streamlit", "run", str(wizard_file)
            ], check=True)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Failed to run setup wizard: {e}")
            return False
        except KeyboardInterrupt:
            self.logger.info("Setup wizard cancelled")
            return True
    
    def _validate_configuration(self) -> bool:
        """Validate current configuration"""
        
        self.logger.info("🧪 Validating configuration...")
        
        if not self.env_file.exists():
            self.logger.error("❌ .env file not found. Run setup wizard first.")
            return False
        
        # Load environment variables
        try:
            from dotenv import load_dotenv
            load_dotenv(self.env_file)
        except ImportError:
            self.logger.warning("⚠️  python-dotenv not installed, using system environment")
        
        # Check required variables
        required_vars = [
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN", 
            "DATABRICKS_HTTP_PATH"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.error(f"❌ Missing required variables: {', '.join(missing_vars)}")
            return False
        
        # Test Databricks connection
        if self._test_databricks_connection():
            self.logger.info("✅ Configuration valid and Databricks connection working")
            return True
        else:
            self.logger.error("❌ Configuration exists but Databricks connection failed")
            return False
    
    def _test_databricks_connection(self) -> bool:
        """Test connection to Databricks"""
        
        try:
            from databricks import sql
            
            host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
            token = os.getenv("DATABRICKS_TOKEN", "")
            http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
            
            if not all([host, token, http_path]):
                return False
            
            with sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=token
            ) as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                return result[0] == 1
                
        except Exception as e:
            self.logger.debug(f"Connection test failed: {e}")
            return False
    
    def _start_autodq(self) -> bool:
        """Start the AutoDQ application"""
        
        if not self._is_configuration_valid():
            self.logger.error("❌ Configuration invalid. Please run setup wizard first.")
            return False
        
        self.logger.info("🚀 Starting AutoDQ...")
        
        app_file = self.project_root / "app.py"
        if not app_file.exists():
            self.logger.error("❌ app.py not found")
            return False
        
        try:
            subprocess.run([
                sys.executable, "-m", "streamlit", "run", str(app_file)
            ], check=True)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Failed to start AutoDQ: {e}")
            return False
        except KeyboardInterrupt:
            self.logger.info("AutoDQ stopped")
            return True
    
    def _view_configuration(self):
        """Display current configuration (with sensitive data masked)"""
        
        print("\n📋 Current Configuration:")
        print("-" * 40)
        
        if self.env_file.exists():
            env_vars = [
                "DATABRICKS_HOST",
                "DATABRICKS_TOKEN",
                "DATABRICKS_HTTP_PATH", 
                "DEFAULT_SCHEMA",
                "DEFAULT_REFRESH_INTERVAL"
            ]
            
            for var in env_vars:
                value = os.getenv(var, "Not set")
                if var == "DATABRICKS_TOKEN" and value != "Not set":
                    value = "***" + value[-4:] if len(value) > 4 else "***"
                print(f"{var}: {value}")
        else:
            print("❌ No configuration file found")
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                print(f"\nClient Config:")
                print(f"  Environment: {config.get('deployment_type', 'Unknown')}")
                print(f"  Configured: {config.get('configured_at', 'Unknown')}")
            except Exception as e:
                print(f"⚠️  Could not read client config: {e}")
    
    def _reset_configuration(self):
        """Reset configuration files"""
        
        confirm = input("\n⚠️  This will delete your current configuration. Are you sure? (y/N): ")
        if confirm.lower() != 'y':
            print("❌ Reset cancelled")
            return
        
        try:
            if self.env_file.exists():
                self.env_file.unlink()
                print("✅ .env file deleted")
            
            if self.config_file.exists():
                self.config_file.unlink()
                print("✅ client_config.json deleted")
            
            print("🔄 Configuration reset complete. Run setup wizard to reconfigure.")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to reset configuration: {e}")
    
    def _check_dependencies(self) -> bool:
        """Check if required dependencies are installed"""
        
        required_packages = [
            "streamlit",
            "pandas", 
            "databricks-sql-connector",
            "python-dotenv"
        ]
        
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
            except ImportError:
                return False
        
        return True
    
    def _is_configuration_valid(self) -> bool:
        """Check if configuration is valid"""
        
        if not self.env_file.exists():
            return False
        
        try:
            from dotenv import load_dotenv
            load_dotenv(self.env_file)
        except ImportError:
            pass
        
        required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]
        return all(os.getenv(var) for var in required_vars)
    
    def _can_connect_to_databricks(self) -> bool:
        """Check if can connect to Databricks"""
        
        if not self._is_configuration_valid():
            return False
        
        return self._test_databricks_connection()

def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(
        description="AutoDQ Client Deployment Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deploy_for_client.py                # Interactive mode
  python deploy_for_client.py --wizard       # Run setup wizard
  python deploy_for_client.py --validate     # Validate configuration
  python deploy_for_client.py --install      # Install dependencies only
        """
    )
    
    parser.add_argument(
        "--wizard", 
        action="store_true",
        help="Run the interactive setup wizard"
    )
    
    parser.add_argument(
        "--validate",
        action="store_true", 
        help="Validate existing configuration"
    )
    
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install dependencies only"
    )
    
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress verbose output"
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Determine mode
    if args.wizard:
        mode = "wizard"
    elif args.validate:
        mode = "validate"
    elif args.install:
        mode = "install"
    else:
        mode = "interactive"
    
    # Run deployment
    deployer = AutoDQClientDeployer()
    success = deployer.deploy(mode)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
