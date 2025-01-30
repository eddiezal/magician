from google.cloud import bigquery
import os

# Set up Google Cloud authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/eddie/OneDrive/code/magician/config/cloud4marketing-281206-f732ef8736c7.json"

# Initialize BigQuery Client
client = bigquery.Client()

# Define project and dataset
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"
TABLES = ["coinbase_coins", "coinbase_hourly_data", "coinbase_hourly_data_v2"]

try:
    for TABLE_ID in TABLES:
        TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        print(f"\n🔍 Fetching sample data from `{TABLE_PATH}`:")

        # Query to fetch 5 sample rows
        query = f"SELECT * FROM `{TABLE_PATH}` LIMIT 5"
        
        df = client.query(query).to_dataframe()

        if df.empty:
            print("⚠️ No data found in this table.")
        else:
            print(df)

except Exception as e:
    print(f"\n❌ Error fetching data: {e}")
