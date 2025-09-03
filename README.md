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

