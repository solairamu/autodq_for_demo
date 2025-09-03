# 🚀 AutoDQ Client Deployment Guide

This guide helps clients deploy AutoDQ in their own Databricks environment. AutoDQ automatically adapts to different deployment scenarios, making it easy to set up regardless of your infrastructure.

## 📋 Quick Start

### Option 1: Automated Setup (Recommended)
```bash
# Download and run the deployment script
python deploy_for_client.py
```

### Option 2: Interactive Setup Wizard
```bash
# Run the setup wizard directly
streamlit run setup_wizard.py
```

### Option 3: Manual Configuration
```bash
# Copy the template and edit manually
cp client_env_template.txt .env
# Edit .env with your Databricks details
# Then run: streamlit run app.py
```

## 🎯 Deployment Scenarios

AutoDQ automatically detects your environment and configures itself accordingly:

| Environment | Setup Required | Authentication |
|------------|----------------|----------------|
| **Databricks Runtime** | ❌ None | ✅ Automatic |
| **Databricks Lakehouse App** | ❌ None | ✅ Automatic |
| **Client Infrastructure** | ✅ Required | 🔧 Manual |
| **Local Development** | ✅ Required | 🔧 Manual |

## 📦 Prerequisites

- **Python 3.8+**
- **Access to a Databricks workspace**
- **SQL Warehouse running in your workspace**
- **Network connectivity to Databricks**

## 🔧 Detailed Setup Instructions

### Step 1: Install Dependencies

```bash
# Install required packages
pip install -r requirements.txt
pip install python-dotenv
```

### Step 2: Gather Databricks Information

You'll need these details from your Databricks workspace:

#### 🌐 Workspace URL
- Log into your Databricks workspace
- Copy the URL from your browser
- Example: `https://your-company.cloud.databricks.com`

#### 🔑 Access Token
1. In Databricks: Click your profile icon (top right)
2. Go to **User Settings**
3. Click **Developer** tab
4. Click **Access Tokens**
5. Click **Generate New Token**
6. Name it "AutoDQ" and set expiration (90 days recommended)
7. Copy the token (starts with "dapi")

#### 🔗 SQL Warehouse HTTP Path
1. In Databricks: Go to **SQL Warehouses** (left sidebar)
2. Select your SQL warehouse (or create one)
3. Click **Connection Details** tab
4. Copy the **HTTP Path** value
5. Example: `/sql/1.0/warehouses/abc123def456`

### Step 3: Configure AutoDQ

Choose one of these methods:

#### Method A: Interactive Setup Wizard (Recommended)

```bash
streamlit run setup_wizard.py
```

The wizard will:
- ✅ Guide you through each configuration step
- 🧪 Test your connection automatically
- 💾 Save your configuration securely
- 🎯 Provide troubleshooting help if needed

#### Method B: Automated Deployment Script

```bash
python deploy_for_client.py
```

Interactive menu options:
1. 📦 Install dependencies
2. ⚙️ Run setup wizard
3. 🧪 Validate configuration
4. 🚀 Start AutoDQ
5. 📋 View configuration
6. 🔄 Reset configuration

#### Method C: Manual Configuration

1. **Copy the template:**
   ```bash
   cp client_env_template.txt .env
   ```

2. **Edit the .env file:**
   ```bash
   # Required settings
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your_databricks_access_token_here
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
   
   # Optional settings
   DEFAULT_SCHEMA=multitable_logistics
   DEFAULT_REFRESH_INTERVAL=10
   ```

3. **Test your configuration:**
   ```bash
   python deploy_for_client.py --validate
   ```

### Step 4: Start AutoDQ

```bash
streamlit run app.py
```

Or use the deployment script:
```bash
python deploy_for_client.py
# Then select option 4: Start AutoDQ
```

## 🔍 Environment Detection

AutoDQ automatically detects your deployment environment:

### ✅ Databricks Runtime
- **Detection**: Checks for `DATABRICKS_RUNTIME_VERSION`, `/databricks/driver`
- **Configuration**: Automatic - no setup needed
- **Authentication**: Uses Databricks runtime credentials

### ✅ Databricks Lakehouse App
- **Detection**: Checks for `DATABRICKS_APP_ID`, `LAKEHOUSE_APP_MODE`
- **Configuration**: Automatic - no setup needed
- **Authentication**: Uses app-level authentication

### ⚙️ Client Infrastructure
- **Detection**: No Databricks runtime detected
- **Configuration**: Manual setup required
- **Authentication**: Uses provided credentials

## 🔐 Security Best Practices

### For Client Deployments

1. **Never commit credentials to version control**
   - The `.env` file is automatically ignored by Git
   - Use `.gitignore` to exclude sensitive files

2. **Use tokens with minimal permissions**
   - Only grant SQL warehouse access
   - Set appropriate token expiration (90 days recommended)

3. **Rotate credentials regularly**
   - Generate new tokens periodically
   - Update configuration using the setup wizard

4. **Network security**
   - Ensure secure network connectivity to Databricks
   - Use VPN if required by your organization

### For Production Deployments

1. **Use Databricks Secrets (Advanced)**
   ```bash
   # Store credentials in Databricks Secrets
   databricks secrets create-scope --scope autodq-secrets
   databricks secrets put --scope autodq-secrets --key databricks-token
   ```

2. **Service Principal Authentication (Enterprise)**
   - Use service principals instead of personal tokens
   - Configure OAuth for enhanced security

## 🧪 Validation and Testing

### Test Your Configuration

```bash
# Validate configuration
python deploy_for_client.py --validate

# Or test manually
python -c "
from environment_detector import environment_detector
config = environment_detector.detect_environment()
print('Environment:', config['environment_type'])
print('Setup required:', config['requires_setup'])
"
```

### Connection Testing

The setup wizard automatically tests:
- ✅ Databricks workspace connectivity
- ✅ SQL warehouse accessibility
- ✅ Schema permissions
- ✅ Token validity

## 🆘 Troubleshooting

### Common Issues

#### ❌ "Authentication failed"
**Cause**: Invalid or expired access token
**Solution**:
- Generate a new access token in Databricks
- Run setup wizard to update configuration
- Check token permissions

#### ❌ "Connection failed"
**Cause**: Network or workspace URL issues
**Solution**:
- Verify workspace URL is correct
- Check network connectivity
- Ensure no firewall blocking

#### ❌ "SQL Warehouse not found"
**Cause**: Warehouse stopped or incorrect path
**Solution**:
- Start your SQL warehouse in Databricks
- Verify HTTP path in configuration
- Check warehouse permissions

#### ❌ "Schema not found"
**Cause**: Schema doesn't exist or no permissions
**Solution**:
- Create the schema in Databricks
- Grant appropriate permissions
- Update DEFAULT_SCHEMA in configuration

### Getting Help

1. **Run the setup wizard** - provides guided troubleshooting
2. **Check configuration** - use `deploy_for_client.py --validate`
3. **Review logs** - check Streamlit console output
4. **Test connection** - use built-in connection testing

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
# Set debug logging
export LOG_LEVEL=debug

# Then run AutoDQ
streamlit run app.py
```

## 📊 Configuration Reference

### Environment Variables

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `DATABRICKS_HOST` | ✅ | Workspace URL | `https://company.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | ✅ | Access token | `dapi1234567890abcdef...` |
| `DATABRICKS_HTTP_PATH` | ✅ | SQL warehouse path | `/sql/1.0/warehouses/abc123` |
| `DEFAULT_SCHEMA` | ❌ | Default schema | `multitable_logistics` |
| `DEFAULT_REFRESH_INTERVAL` | ❌ | Refresh interval (min) | `10` |
| `DATABRICKS_SSL_VERIFY` | ❌ | SSL verification | `true` |

### Configuration Files

| File | Purpose | Location |
|------|---------|----------|
| `.env` | Environment variables | Project root |
| `client_config.json` | Client metadata | Project root |
| `client_env_template.txt` | Configuration template | Project root |

## 🚀 Advanced Deployment

### Docker Deployment

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN pip install python-dotenv

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: autodq
spec:
  replicas: 1
  selector:
    matchLabels:
      app: autodq
  template:
    metadata:
      labels:
        app: autodq
    spec:
      containers:
      - name: autodq
        image: autodq:latest
        ports:
        - containerPort: 8501
        env:
        - name: DATABRICKS_HOST
          valueFrom:
            secretKeyRef:
              name: databricks-secret
              key: host
        - name: DATABRICKS_TOKEN
          valueFrom:
            secretKeyRef:
              name: databricks-secret
              key: token
        - name: DATABRICKS_HTTP_PATH
          valueFrom:
            secretKeyRef:
              name: databricks-secret
              key: http-path
```

## 📞 Support

### Self-Service Options

1. **Interactive Setup Wizard**: `streamlit run setup_wizard.py`
2. **Deployment Script**: `python deploy_for_client.py`
3. **Configuration Validation**: `python deploy_for_client.py --validate`
4. **Documentation**: Review this guide and inline help

### Getting Additional Help

- Check error messages carefully - they include specific guidance
- Use the built-in troubleshooting features
- Review Databricks workspace permissions and settings
- Ensure SQL warehouse is running and accessible

---

## 🎉 Success!

Once configured, AutoDQ will:
- ✅ Automatically connect to your Databricks workspace
- 📊 Load your data quality validation results
- 🔍 Provide interactive data quality insights
- 📈 Track and manage data quality issues
- 🤖 Offer smart rule suggestions

**Ready to explore your data quality? Start AutoDQ and begin your data quality journey!**
