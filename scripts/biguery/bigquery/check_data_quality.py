from google.cloud import bigquery
import os
import pandas as pd

# Set up authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/eddie/OneDrive/code/magician/config/cloud4marketing-281206-f732ef8736c7.json"

# Initialize BigQuery Client
client = bigquery.Client()

# Define table
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"
TABLE_ID = "coinbase_hourly_prices"
TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Define known assets (optional)
KNOWN_ASSETS = ["BTC", "ETH", "SOL", "ADA", "XRP", "LTC", "DOGE", "AVAX"]

# Define SQL queries for data quality checks
QUERIES = {
    "missing_values": f"""
        SELECT COUNT(*) AS missing_values
        FROM `{TABLE_PATH}`
        WHERE asset IS NULL 
        OR timestamp IS NULL 
        OR open IS NULL 
        OR close IS NULL 
        OR high IS NULL 
        OR low IS NULL 
        OR volume IS NULL;
    """,
    "duplicate_timestamps": f"""
        SELECT asset, timestamp, COUNT(*) as count
        FROM `{TABLE_PATH}`
        GROUP BY asset, timestamp
        HAVING COUNT(*) > 1;
    """,
    "time_gaps": f"""
        WITH time_series AS (
            SELECT asset, timestamp,
            LAG(timestamp) OVER (PARTITION BY asset ORDER BY timestamp) AS prev_timestamp
            FROM `{TABLE_PATH}`
        )
        SELECT asset, timestamp, prev_timestamp, 
        TIMESTAMP_DIFF(timestamp, prev_timestamp, HOUR) AS hour_diff
        FROM time_series
        WHERE hour_diff > 1;
    """,
    "anomalies": f"""
        WITH price_changes AS (
            SELECT asset, timestamp, close,
            LAG(close) OVER (PARTITION BY asset ORDER BY timestamp) AS prev_close
            FROM `{TABLE_PATH}`
        )
        SELECT asset, timestamp, prev_close, close, 
        ROUND(ABS((close - prev_close) / prev_close) * 100, 2) AS pct_change
        FROM price_changes
        WHERE pct_change > 20;
    """,
    "outliers_in_volume": f"""
        WITH volume_stats AS (
            SELECT asset, 
                   PERCENTILE_CONT(volume, 0.99) OVER (PARTITION BY asset) AS vol_threshold
            FROM `{TABLE_PATH}`
        )
        SELECT t.*
        FROM `{TABLE_PATH}` t
        JOIN volume_stats v ON t.asset = v.asset
        WHERE t.volume > v.vol_threshold * 2;
    """,
    "negative_or_zero_prices": f"""
        SELECT *
        FROM `{TABLE_PATH}`
        WHERE open <= 0 OR close <= 0 OR high <= 0 OR low <= 0;
    """,
    "spikes_in_volume": f"""
        WITH volume_changes AS (
            SELECT asset, timestamp, volume,
            LAG(volume) OVER (PARTITION BY asset ORDER BY timestamp) AS prev_volume
            FROM `{TABLE_PATH}`
        )
        SELECT asset, timestamp, prev_volume, volume, 
        ROUND(volume / NULLIF(prev_volume, 0), 2) AS volume_ratio
        FROM volume_changes
        WHERE volume_ratio > 10;
    """,
    "invalid_asset_symbols": f"""
        SELECT DISTINCT asset
        FROM `{TABLE_PATH}`
        WHERE asset NOT IN ({','.join([f"'{a}'" for a in KNOWN_ASSETS])});
    """,
    "missing_consecutive_hours": f"""
        WITH time_gaps AS (
            SELECT asset, timestamp,
            LAG(timestamp) OVER (PARTITION BY asset ORDER BY timestamp) AS prev_timestamp
            FROM `{TABLE_PATH}`
        )
        SELECT asset, timestamp, prev_timestamp, 
        TIMESTAMP_DIFF(timestamp, prev_timestamp, HOUR) AS hour_diff
        FROM time_gaps
        WHERE hour_diff >= 5;
    """
}

try:
    print("\n🔍 Running Data Quality Checks...\n")

    for check, query in QUERIES.items():
        df = client.query(query).to_dataframe()

        print(f"\n📌 {check.replace('_', ' ').title()}:")
        if df.empty:
            print("✅ No issues found!")
        else:
            print(df)

except Exception as e:
    print(f"\n❌ Error running data quality checks: {e}")
