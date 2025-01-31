import sys
import os

# Get absolute path to the project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

# Add project root to Python path
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Now import BigQuery config
from config import client, TABLE_PATH  # Ensure correct import


print("\n🔍 Running Data Validation Checks...\n")

# Define validation queries
VALIDATION_QUERIES = {
    "missing_values": f"""
        SELECT COUNT(*) AS missing_count
        FROM `{TABLE_PATH}`
        WHERE asset IS NULL OR timestamp IS NULL 
        OR open_price IS NULL OR close_price IS NULL 
        OR high_price IS NULL OR low_price IS NULL 
        OR volume IS NULL
    """,
    "duplicate_timestamps": f"""
        SELECT asset, timestamp, COUNT(*) AS duplicate_count
        FROM `{TABLE_PATH}`
        GROUP BY asset, timestamp
        HAVING COUNT(*) > 1
    """,
    "negative_prices": f"""
        SELECT COUNT(*) AS negative_price_count
        FROM `{TABLE_PATH}`
        WHERE open_price < 0 OR close_price < 0 
        OR high_price < 0 OR low_price < 0
    """,
    "negative_volume": f"""
        SELECT COUNT(*) AS negative_volume_count
        FROM `{TABLE_PATH}`
        WHERE volume < 0
    """
}

# Run validation
issues_found = False

for check, query in VALIDATION_QUERIES.items():
    df = client.query(query).to_dataframe()

    print(f"\n📌 {check.replace('_', ' ').title()}:")
    if df.empty or df.iloc[0, 0] == 0:
        print("✅ No issues found!")
    else:
        print(df)
        issues_found = True

if not issues_found:
    print("\n🎉 Data looks clean! Ready for feature engineering.")
else:
    print("\n⚠️ Data quality issues found. Please review the results above.")

print("\n✅ Validation completed!")
