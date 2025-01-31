from google.cloud import bigquery
import os

# Set up authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/eddie/OneDrive/code/magician/config/cloud4marketing-281206-f732ef8736c7.json"

# Initialize BigQuery Client
client = bigquery.Client()

# Define dataset
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"

try:
    tables = client.list_tables(f"{PROJECT_ID}.{DATASET_ID}")
    
    print("\n✅ Tables currently in `crypto_price` dataset:")
    for table in tables:
        print(f"- {table.table_id}")

except Exception as e:
    print(f"\n❌ Error fetching tables: {e}")
