from google.cloud import bigquery
import os

# Set up authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/eddie/OneDrive/code/magician/config/cloud4marketing-281206-f732ef8736c7.json"

# Initialize BigQuery Client
client = bigquery.Client()

# Define table paths
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"
OLD_TABLE = f"{PROJECT_ID}.{DATASET_ID}.coinbase_hourly_prices"
NEW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.coinbase_hourly_prices_v2"

# SQL Query to rename columns
QUERY = f"""
CREATE OR REPLACE TABLE `{NEW_TABLE}` AS
SELECT 
    coin_id AS asset,
    timestamp,
    low AS low_price,
    high AS high_price,
    open AS open_price,
    close AS close_price,
    volume
FROM `{OLD_TABLE}`;
"""

try:
    print("\n🔄 Renaming columns in `coinbase_hourly_prices`...")

    # Run query to create a new table with renamed columns
    client.query(QUERY).result()

    # Delete old table
    print(f"\n🗑️ Deleting old table `{OLD_TABLE}`...")
    client.delete_table(OLD_TABLE, not_found_ok=True)

    # Copy `NEW_TABLE` back to `OLD_TABLE`
    print(f"\n📄 Copying `{NEW_TABLE}` back to `{OLD_TABLE}`...")
    job = client.copy_table(NEW_TABLE, OLD_TABLE)
    job.result()  # Wait for the job to complete

    # Delete temporary `NEW_TABLE`
    print(f"\n🗑️ Deleting temporary table `{NEW_TABLE}`...")
    client.delete_table(NEW_TABLE, not_found_ok=True)

    print("\n✅ Columns successfully renamed in `coinbase_hourly_prices`!")

except Exception as e:
    print(f"\n❌ Error renaming columns: {e}")
