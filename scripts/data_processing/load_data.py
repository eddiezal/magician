import pandas as pd
from sklearn.model_selection import train_test_split  
import os
from google.cloud import bigquery  # ✅ Ensure this import is present
from scripts.indicators.compute_indicators import get_optimal_timeframe  # Import the new function

# 🔥 Set the path to your credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\eddie\OneDrive\code\magician\config\cloud_credentials.json"

# ✅ Now initialize the BigQuery client
client = bigquery.Client()

def load_data():
    """
    🍽 **load_data() – Your Data, Served Hot**
    
    - Loads historical crypto data from BigQuery
    - Splits it into training/testing sets
    - Because training on fresh data is like cooking with rotten vegetables

    🚨 NOTE: This assumes data is already cleaned!
    """

    print("📡 Fetching data from BigQuery...")

    client = bigquery.Client()
    query = """
        SELECT * FROM `cloud4marketing-281206.crypto_price.technical_indicators`
        WHERE DATETIME(timestamp) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 MONTH)
    """
    df = client.query(query).to_dataframe()

    # Fetch optimal timeframes for each asset
    optimal_timeframes = get_optimal_timeframe(df)

    # 📊 Split into X (features) and y (target)
    if "target" not in df.columns:
        raise ValueError("🚨 'target' column is missing! Did we forget to define what we're predicting?")

    X = df.drop(columns=["asset", "timestamp", "target"])  
    y = df["target"]  

    print(f"✅ Data loaded! Total rows: {df.shape[0]}, Features: {X.shape[1]}")
    print(f"🔍 Sample Data:\n{df.head(5)}\n")
    
    return train_test_split(X, y, test_size=0.2, random_state=42)

# If run as a script, execute and print a sample
if __name__ == "__main__":
    X_train, X_test, y_train, y_test = load_data()
    print("📊 Training Sample:")
    print(X_train.head(3))
    print("🎯 Target Sample:")
    print(y_train.head(3))
