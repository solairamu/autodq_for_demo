### For Clients: Quick Deployment

AutoDQ automatically adapts to your Databricks environment. Choose your preferred setup method:

#### 🚀 **Option 1: Automated Setup (Recommended)**
```bash
python deploy_for_client.py
```

#### 🧙 **Option 2: Interactive Setup Wizard**
```bash
streamlit run setup_wizard.py
```

#### ⚙️ **Option 3: Manual Configuration**
```bash
cp client_env_template.txt .env
# Edit .env with your Databricks details
streamlit run app.py
```

> **📖 Full Instructions**: See [CLIENT_DEPLOYMENT.md](CLIENT_DEPLOYMENT.md) for complete setup guide

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install python-dotenv
   ```

2. **Create environment file**:
   ```bash
   Create a file called .env inside root directory, 
   Have it copy the format of env_template.txt
   
   
   Edit .env with your actual Databricks credentials
   Replace the placeholder values with your real:
   - DATABRICKS_HOST (your workspace URL)
   - DATABRICKS_TOKEN (your access token)
   - DATABRICKS_HTTP_PATH (your SQL warehouse path)
   ```

3. **Run locally**:
   ```bash
   streamlit run app.py
   ```

> **⚠️ Important**: Never commit your `.env` file with real credentials to version control. The `.env` file is already included in `.gitignore` for security.

### Testing

Test all functionality without needing a Databricks connection:

```bash
python3 test_everything.py
```

This single command:
- ✅ Tests all functionality (data loading, cleaning, anomaly detection)
- ✅ Tests all edge cases (Unicode, SQL reserved words, extreme values)  
- ✅ Generates comprehensive test data (19+ datasets)
- ✅ Tests 6 industry patterns (Enterprise, SaaS, E-commerce, etc.)
- ✅ Covers 95%+ of real-world scenarios

For detailed information, see `SIMPLE_TESTING_README.md`.

