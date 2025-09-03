# AutoDQ Deployment Guide

This guide explains how to deploy AutoDQ in different environments.

## 🏠 Local Development vs 📦 Marketplace Deployment

### Understanding the Difference

**Local Development:**
- Uses **your specific** Databricks workspace credentials
- For testing and development only
- Requires manual configuration of connection parameters

**Marketplace Deployment:**
- Automatically uses the **end user's** Databricks workspace
- No hardcoded credentials needed
- The app runs **inside** the user's Databricks environment with automatic authentication

## 🔧 Local Development Setup

### Prerequisites
- Python 3.8+
- Access to a Databricks workspace
- SQL Warehouse running in your Databricks workspace

### Step 1: Environment Setup

1. **Create a `.env` file** in the project root:
```bash
# AutoDQ for Databricks - Environment Variables
# LOCAL DEVELOPMENT ONLY - DO NOT COMMIT TO VERSION CONTROL

DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_access_token_here
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DEFAULT_SCHEMA=your_schema_name
DEFAULT_REFRESH_INTERVAL=10
DATABRICKS_APP_PORT=8501
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
pip install python-dotenv
```

3. **Run locally:**
```bash
streamlit run app.py
```

### Step 2: Get Your Connection Parameters

**Databricks Workspace URL:**
- Go to your Databricks workspace
- Copy the URL from your browser (e.g., `https://abc123.cloud.databricks.com`)

**Access Token:**
- In Databricks: User Settings → Developer → Access Tokens
- Click "Generate New Token"
- Copy the generated token (starts with `dapi`)

**SQL Warehouse HTTP Path:**
- Go to SQL Warehouses in your Databricks workspace
- Select your warehouse → Connection Details
- Copy the HTTP Path (e.g., `/sql/1.0/warehouses/abc123def456`)

## 📦 Databricks Marketplace Deployment

### How Marketplace Apps Work

When you deploy to the Databricks Marketplace:

1. **Automatic Authentication**: The app automatically uses the installing user's Databricks workspace credentials
2. **No Manual Configuration**: Users don't need to provide connection parameters
3. **Workspace Integration**: The app runs within the user's Databricks environment
4. **Secure**: No credentials are exposed or shared between workspaces

### Deployment Steps

1. **Prepare the Bundle:**
```bash
# Ensure your databricks.yml is configured for marketplace
# (Connection parameters are automatically handled)
```

2. **Deploy to Your Workspace First (Testing):**
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure

# Deploy the app
databricks bundle deploy

# Run the app
databricks bundle run autodq
```

3. **Submit to Marketplace:**
- Package your application
- Submit through Databricks Partner Portal
- Users can install directly from the Marketplace

### Key Differences in Marketplace Deployment

| Aspect | Local Development | Marketplace Deployment |
|--------|------------------|------------------------|
| **Authentication** | Manual `.env` file | Automatic (Databricks runtime) |
| **Connection** | Your workspace only | Each user's workspace |
| **Credentials** | Hardcoded for testing | Dynamic per installation |
| **Configuration** | Manual setup required | Automatic workspace integration |

## 🔒 Security Considerations

### Local Development
- **Never commit** `.env` files to version control
- Use `.gitignore` to exclude sensitive files
- Rotate tokens regularly

### Marketplace Deployment
- **No hardcoded credentials** in the application
- Databricks handles all authentication automatically
- Users' data stays in their workspace

## 🚀 Testing Before Marketplace Submission

1. **Local Testing**: Verify the app works with your own data
2. **Workspace Deployment**: Deploy to your Databricks workspace first
3. **User Acceptance**: Test with different schemas and data structures
4. **Performance**: Ensure the app handles large datasets efficiently

## 📋 Marketplace Submission Checklist

- [ ] App works with sample data
- [ ] No hardcoded credentials or workspace-specific configurations
- [ ] Proper error handling for missing data/tables
- [ ] Comprehensive documentation
- [ ] App description and screenshots
- [ ] Testing with different Databricks workspace configurations

## 🆘 Troubleshooting

### Local Development Issues
- **Connection Failed**: Check your `.env` file credentials
- **Token Expired**: Generate a new access token
- **SQL Warehouse Stopped**: Ensure your warehouse is running

### Marketplace Deployment Issues
- **Authentication Failed**: Ensure the app detects Databricks runtime correctly
- **Schema Not Found**: Make sure users have access to the required schemas
- **Permission Denied**: Verify the app has necessary Unity Catalog permissions

## 📞 Support

For deployment assistance:
1. Check this guide first
2. Review error messages carefully
3. Test locally before marketplace submission
4. Contact support with specific error details 