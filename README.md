# AutoDQ for Databricks - Lakehouse App

AutoDQ is a comprehensive Data Quality and Validation platform designed specifically for Databricks environments. This version is packaged as a Databricks Lakehouse App for easy deployment and management.

## 🚀 Features

- **Real-time Data Quality Monitoring** - Monitor validation results across your Databricks tables
- **Smart Rule Assistant** - AI-powered natural language rule creation
- **Interactive Dashboards** - Rich visualizations for data quality insights
- **Data Intelligence Hub** - Advanced analytics on data quality patterns
- **Anomaly Detection** - Statistical analysis for outlier identification
- **Action Tracking** - Issue management and resolution workflow
- **Seamless Databricks Integration** - Native Unity Catalog and SQL Warehouse support

## ⚡ Quick Start for Local Development

1. **Clone the repository**:
   ```bash
   git clone https://github.com/solairamu/autodq_for_demo.git
   cd autodq_for_demo
   ```

2. **Set up your environment**:
   ```bash
   pip install -r requirements.txt python-dotenv
   cp env_example.txt .env
   # Edit .env with your Databricks credentials
   ```

3. **Run the application**:
   ```bash
   streamlit run app.py
   ```

## 📋 Prerequisites

Before deploying AutoDQ as a Lakehouse App, ensure you have:

1. **Databricks Workspace** with Unity Catalog enabled
2. **SQL Warehouse** running and accessible
3. **Personal Access Token** or Service Principal with appropriate permissions
4. **Required Tables** in your Databricks environment:
   - `gx_validation_results_cleaned_combined`
   - `user_defined_validation_log_final_for_dashboard` (optional)

## 🛠️ Installation

### Step 1: Prepare Your Environment

1. **Clone or download** this repository
2. **Ensure all required files** are present:
   ```
   autodq_for_demo/
   ├── app.py
   ├── requirements.txt
   ├── databricks.yml
   ├── config.py
   ├── data_loader.py
   ├── dq_dashboard.py
   ├── smart_rule_assistant.py
   ├── data_intelligence.py
   ├── settings.py
   ├── product_overview.py
   ├── alerts.py
   ├── anomaly_detection.py
   ├── coverage.py
   ├── data_cleaning.py
   ├── dq_tracker.py
   ├── schema_inference.py
   ├── utils.py
   └── README.md
   ```

### Step 2: Configure Environment Variables

Set up the following environment variables in your Databricks workspace:

#### Required Variables:
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

#### Optional Variables:
```bash
DEFAULT_SCHEMA=multitable_logistics
DEFAULT_REFRESH_INTERVAL=10
DATABRICKS_JOB_ID=your-job-id
```

### Step 3: Deploy as Lakehouse App

1. **Upload the project** to your Databricks workspace
2. **Configure secrets** for sensitive data (recommended):
   ```bash
   # Create secrets for secure credential management
   databricks secrets create-scope autodq-secrets
   databricks secrets put-secret autodq-secrets databricks-token
   databricks secrets put-secret autodq-secrets databricks-host
   databricks secrets put-secret autodq-secrets databricks-http-path
   ```

3. **Deploy the app** using Databricks CLI or UI:
   ```bash
   databricks bundle deploy
   ```

4. **Access your app** via the Databricks Apps interface

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | Yes | - |
| `DATABRICKS_TOKEN` | Access token for authentication | Yes | - |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path | Yes | - |
| `DEFAULT_SCHEMA` | Default schema for validation tables | No | `multitable_logistics` |
| `DEFAULT_REFRESH_INTERVAL` | Refresh interval in minutes | No | `10` |
| `DATABRICKS_JOB_ID` | Job ID for smart rule execution | No | - |

### Data Requirements

AutoDQ expects the following table structure in your Databricks environment:

#### Main Validation Results Table
```sql
-- Table: {schema}.gx_validation_results_cleaned_combined
CREATE TABLE {schema}.gx_validation_results_cleaned_combined (
    Run_Timestamp TIMESTAMP,
    Table STRING,
    Column STRING,
    Rule STRING,
    Rule_Display_Name STRING,
    Status STRING,
    Metric STRING,
    Failed_Row_ID STRING,
    Failed_Value STRING,
    Failure_Type STRING
);
```

#### User-Defined Validation Log (Optional)
```sql
-- Table: {schema}.user_defined_validation_log_final_for_dashboard
CREATE TABLE {schema}.user_defined_validation_log_final_for_dashboard (
    Execution_ID STRING,
    Run_Timestamp TIMESTAMP,
    Rule_Display_Name STRING,
    Status STRING,
    Table STRING,
    Column STRING,
    Metric STRING
);
```

## 🎯 Usage

### Accessing the Application

Once deployed, access AutoDQ through:
1. **Databricks Apps** section in your workspace
2. **Direct URL** provided after deployment
3. **Shared links** for team collaboration

### Main Features

#### 1. Validation Dashboard
- Real-time monitoring of data quality checks
- Interactive filtering and visualization
- Trend analysis and failure tracking

#### 2. Smart Rule Assistant
- Natural language rule creation
- AI-powered validation logic
- Automated rule execution

#### 3. Data Intelligence Hub
- Advanced analytics on data quality patterns
- Cleaning status summaries
- Historical trend analysis

#### 4. Settings & Configuration
- Connection management
- Refresh scheduling
- Schema exploration

## 🔧 Troubleshooting

### Common Issues

#### Connection Errors
```
Failed to connect to Databricks
```
**Solution:** Check your environment variables and ensure the SQL Warehouse is running.

#### Missing Tables
```
Table 'gx_validation_results_cleaned_combined' not found
```
**Solution:** Ensure the required validation tables exist in your specified schema.

#### Permission Errors
```
Access denied to schema/table
```
**Solution:** Verify your access token has appropriate permissions for Unity Catalog and SQL Warehouse.

### Debug Mode

Enable debug logging by setting:
```bash
STREAMLIT_LOGGER_LEVEL=debug
```

### Health Check

Use the built-in connection test in the Settings page to verify:
- ✅ Databricks connectivity
- ✅ Table accessibility  
- ✅ Query execution

## 📊 Data Quality Rules

AutoDQ supports the following validation rule types:

### High Priority Rules
- **No Nulls** - Detect missing values
- **Unique Values** - Identify duplicates
- **Primary Key Present** - Ensure key completeness
- **Foreign Key Valid** - Validate referential integrity

### Normal Priority Rules
- **Range OK** - Check value boundaries
- **Valid Type** - Verify data types
- **Format Match** - Pattern validation
- **Column Present** - Schema validation
- **Allowed Values** - Domain validation
- **Valid Date** - Date format validation

## 🔐 Security

### Best Practices

1. **Use Service Principals** for production deployments
2. **Store secrets securely** using Databricks Secrets
3. **Apply least privilege** access patterns
4. **Enable audit logging** for compliance
5. **Regular token rotation** for security

### Access Control

AutoDQ inherits your Databricks workspace security:
- **Unity Catalog permissions** for data access
- **SQL Warehouse permissions** for query execution
- **Workspace-level access** for app usage

## 🚀 Performance Optimization

### Recommended Settings

1. **SQL Warehouse Sizing**: Medium or larger for production
2. **Refresh Intervals**: 10-30 minutes for real-time monitoring
3. **Query Optimization**: Use filtered views for large datasets
4. **Caching**: Enable Streamlit caching for better performance

### Scaling Considerations

- **Serverless Compute**: Automatically scales with demand
- **Multi-User Support**: Concurrent access without performance degradation
- **Resource Management**: Configurable through Databricks Apps settings

## 🧪 Development

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install python-dotenv
   ```

2. **Create environment file**:
   ```bash
   # Copy the example environment file
   cp env_example.txt .env
   
   # Edit .env with your actual Databricks credentials
   # Replace the placeholder values with your real:
   # - DATABRICKS_HOST (your workspace URL)
   # - DATABRICKS_TOKEN (your access token)
   # - DATABRICKS_HTTP_PATH (your SQL warehouse path)
   ```

3. **Run locally**:
   ```bash
   streamlit run app.py
   ```

> **⚠️ Important**: Never commit your `.env` file with real credentials to version control. The `.env` file is already included in `.gitignore` for security.

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📞 Support

For support and questions:

1. **Documentation**: Check this README and inline help
2. **Connection Issues**: Use the Settings page connection test
3. **Feature Requests**: Submit through your organization's channels
4. **Bug Reports**: Include logs and environment details

## 📄 License

This project is licensed under the terms specified by your organization. Please refer to your internal documentation for licensing details.

## 🔄 Changelog

### Version 1.0.0
- Initial Lakehouse App release
- Environment variable configuration
- Streamlit UI compatibility
- Full Databricks integration
- Security and error handling improvements 