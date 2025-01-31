import os
from google.cloud import bigquery

# Set up authentication for BigQuery
SERVICE_ACCOUNT_PATH = "C:/Users/eddie/OneDrive/code/magician/config/cloud_credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

# Initialize BigQuery Client
client = bigquery.Client()

# Define dataset and table names
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"
TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.coinbase_hourly_prices"
