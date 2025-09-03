# config.py

import os

# Load environment variables from .env file for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not available in production

RULE_CONFIG = {
    "No Nulls": {
        "action": "Check and fill missing values from source systems or apply defaults.",
        "priority": "High"
    },
    "Unique Values": {
        "action": "Investigate and deduplicate conflicting or repeated entries.",
        "priority": "High"
    },
    "Primary Key Present": {
        "action": "Source system must provide a primary key for this record.",
        "priority": "High"
    },
    "Foreign Key Valid": {
        "action": "Ensure the foreign key in this record exists in the referenced table.",
        "priority": "High"
    },
    "Range OK": {
        "action": "Verify business rules for min/max boundaries and update thresholds if needed.",
        "priority": "Normal"
    },
    "Valid Type": {
        "action": "Ensure consistent data types across systems.",
        "priority": "Normal"
    },
    "Format Match": {
        "action": "Correct formatting issues according to the specified regular expression.",
        "priority": "Normal"
    },
    "Column Present": {
        "action": "Ensure required column exists in the dataset.",
        "priority": "Normal"
    },
    "Allowed Values": {
        "action": "Cross-check values against a valid domain list.",
        "priority": "Normal"
    },
    "Valid Date": {
        "action": "Correct unparseable date values or enforce consistent date formats.",
        "priority": "Normal"
    }
}

# Configuration from environment variables with defaults
DEFAULT_SCHEMA = os.getenv("DEFAULT_SCHEMA", "multitable_logistics")
DEFAULT_REFRESH_INTERVAL = int(os.getenv("DEFAULT_REFRESH_INTERVAL", "10"))

DQ_STATUS_OPTIONS = [
    "Open", "In Progress", "Waiting on Data Source", "Pending Review",
    "Resolved", "Verified", "Ignored", "Closed"
]

# Databricks connection configuration (for modules that need it)
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")

# SSL Configuration for local development
# Set to "false" to disable SSL verification (useful for self-signed certificates)
DATABRICKS_SSL_VERIFY = os.getenv("DATABRICKS_SSL_VERIFY", "false").lower() == "true"

# App configuration
APP_PORT = int(os.getenv("DATABRICKS_APP_PORT", "8501"))
