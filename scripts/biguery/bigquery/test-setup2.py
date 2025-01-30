from google.cloud import bigquery
import os

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/eddie/OneDrive/code/magician/config/cloud4marketing-281206-f732ef8736c7.json"


# Confirm environment variable is set
print("GOOGLE_APPLICATION_CREDENTIALS:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# Test BigQuery connection
client = bigquery.Client()
print("✅ Successfully connected to BigQuery!")
