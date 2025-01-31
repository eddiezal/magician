import sys
import os
import pandas as pd
import matplotlib.pyplot as plt

# Ensure Python can find the `scripts/` folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts")))

# Import BigQuery config
from config import client, TABLE_PATH

# Fetch data for BTC
query = f"""
    SELECT asset, timestamp, open_price, high_price, low_price, close_price, volume
    FROM `{TABLE_PATH}`
    WHERE asset = 'BTC'
    ORDER BY timestamp DESC
    LIMIT 1000
"""
df = client.query(query).to_dataframe()

# Convert timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Debugging: Print column names to ensure they match
print("\n📌 Columns returned from BigQuery:", df.columns.tolist())

# Plot price trends
plt.figure(figsize=(12, 6))
plt.plot(df["timestamp"], df["close_price"], label="BTC Close Price", color="blue")
plt.xlabel("Timestamp")
plt.ylabel("Close Price")
plt.title("BTC Hourly Close Price Over Time")
plt.legend()
plt.xticks(rotation=45)
plt.show()
