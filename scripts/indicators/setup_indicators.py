import os
import pandas as pd
from google.cloud import bigquery

# 🌐 BigQuery Configuration
PROJECT_ID = "cloud4marketing-281206"  # Your Google Cloud project ID
DATASET_ID = "crypto_price"  # Dataset where the technical indicators are stored
TECHNICALS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.technical_indicators"  # Full path to the BigQuery table

# 📂 Local File Storage
LOCAL_FILE = "technical_indicators.parquet"  # Cached file to avoid repeated BigQuery queries

def load_indicators():
    """
    Loads technical indicators data either from:
    1️⃣ A **local Parquet file** (for faster loading if available)
    2️⃣ **BigQuery** (if the local file doesn't exist or needs refreshing)

    Returns:
        pd.DataFrame: A DataFrame containing technical indicators for various assets.
    """
    
    # ✅ Check if the local Parquet file exists (faster load)
    if os.path.exists(LOCAL_FILE):
        print("✅ Loaded indicators from local storage!")
        return pd.read_parquet(LOCAL_FILE)
    
    # ⚠️ If local file is missing, fetch data from BigQuery
    print("⚠️ Local file not found, querying BigQuery instead.")

    # 🔐 Initialize BigQuery Client
    client = bigquery.Client()

    # 🏦 Fetch Data from BigQuery (Last 6 Months Only)
    query = f"""
        SELECT * FROM `{TECHNICALS_TABLE}`
        WHERE DATETIME(timestamp) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 MONTH)
    """
    df = client.query(query).to_dataframe()

    # 💾 Save the results locally to reduce future queries
    df.to_parquet(LOCAL_FILE)
    print("✅ Technical indicators saved locally!")

    return df

if __name__ == "__main__":
    # 🏃 Run as a script: Load the indicators and preview data
    df = load_indicators()
    print(df.head())  # Quick data check
