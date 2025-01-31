import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from pandas_gbq import to_gbq
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import sklearn
print(sklearn.__version__)



# 🌐 BigQuery Configuration
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"
RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.coinbase_hourly_prices"
TECHNICALS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.technical_indicators"
OPTIMAL_TIMEFRAMES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.optimal_timeframes"

# 🔐 Set authentication
CREDENTIALS_PATH = r"C:\Users\eddie\OneDrive\code\magician\config\cloud_credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
client = bigquery.Client()

# 🟢 Step 1: Load Data from BigQuery (Last 6 Months for Freshness)
query = f"""
    SELECT asset, timestamp, open_price, high_price, low_price, close_price, volume
    FROM `{RAW_TABLE}`
    WHERE DATETIME(timestamp) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 MONTH)
    ORDER BY asset, timestamp
"""
df = client.query(query).to_dataframe()

# 🔍 Debug: Check Unique Assets Before Processing
print(f"🔍 Unique assets count before processing: {df['asset'].nunique()}")
print(df['asset'].value_counts())  # See how many rows exist per asset
print(df[['asset', 'timestamp']].head(50))  # Sample data check

# 📈 Step 2: Compute Technical Indicators (Per Asset)
def compute_indicators(group):
    """Computes a set of technical indicators for each asset using Pandas & NumPy."""
    
    print(f"🛠️ Processing asset: {group['asset'].iloc[0]}, Rows: {len(group)}")  # Debug

    # 🔹 Moving Averages
    group["ema_20"] = group["close_price"].ewm(span=20, adjust=False).mean()
    group["ema_50"] = group["close_price"].ewm(span=50, adjust=False).mean()

    # 🔹 Relative Strength Index (RSI)
    delta = group["close_price"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14, min_periods=1).mean()
    avg_loss = pd.Series(loss).rolling(window=14, min_periods=1).mean()
    rs = avg_gain / avg_loss
    group["rsi_14"] = 100 - (100 / (1 + rs))

    # 🔹 Bollinger Bands
    group["bollinger_mid"] = group["close_price"].rolling(window=20).mean()
    group["bollinger_std"] = group["close_price"].rolling(window=20).std()
    group["bollinger_upper"] = group["bollinger_mid"] + (group["bollinger_std"] * 2)
    group["bollinger_lower"] = group["bollinger_mid"] - (group["bollinger_std"] * 2)

    # 🔹 MACD
    short_ema = group["close_price"].ewm(span=12, adjust=False).mean()
    long_ema = group["close_price"].ewm(span=26, adjust=False).mean()
    group["macd"] = short_ema - long_ema
    group["macd_signal"] = group["macd"].ewm(span=9, adjust=False).mean()

    # 🔹 Money Flow Index (MFI)
    typical_price = (group["high_price"] + group["low_price"] + group["close_price"]) / 3
    money_flow = typical_price * group["volume"]
    pos_flow = money_flow.where(typical_price > typical_price.shift(), 0)
    neg_flow = money_flow.where(typical_price < typical_price.shift(), 0)
    money_ratio = pos_flow.rolling(14).sum() / neg_flow.rolling(14).sum()
    group["mfi_14"] = 100 - (100 / (1 + money_ratio))

    # 🔹 Z-Score (Mean Reversion)
    group["z_score"] = (group["close_price"] - group["close_price"].rolling(20).mean()) / group["close_price"].rolling(20).std()

    # 🔹 Average True Range (ATR)
    high_low = group["high_price"] - group["low_price"]
    high_close = np.abs(group["high_price"] - group["close_price"].shift())
    low_close = np.abs(group["low_price"] - group["close_price"].shift())

    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    group["atr_14"] = true_range.rolling(14).mean()

    return group

# 🛠 Apply Indicator Computation
df = df.groupby("asset", group_keys=True).apply(compute_indicators).reset_index(drop=True)

# 🔍 Debug: Check Unique Assets After Processing
print(f"🔍 Unique assets count after processing: {df['asset'].nunique()}")
print(df['asset'].unique())  # List all unique asset names
print(df[['asset', 'timestamp']].head(50))  # Sample check

df.dropna(inplace=True)

# 🛠 Step 3: Upload Processed Data to BigQuery
to_gbq(df, TECHNICALS_TABLE, project_id=PROJECT_ID, if_exists="replace")

print("✅ Technical indicators computed & uploaded to BigQuery!")
print([col for col in df.columns if 'atr' in col])

# 🟢 Step 4: Define Function to Get Optimal Timeframe
def get_optimal_timeframe(asset_data):
    """
    Analyzes past price movements to determine the best timeframe per asset.
    Computes predictive performance metrics (MAE, MSE, R², RMSE) for different timeframes.
    """
    timeframes = [1, 2, 4, 6, 12, 24]  # Different timeframes to test (in hours)
    results = []

    for timeframe in timeframes:
        asset_data[f"target_{timeframe}h"] = asset_data["close_price"].shift(-timeframe)
        asset_data.dropna(inplace=True)

        X = asset_data.drop(columns=[f"target_{timeframe}h", "asset", "timestamp"])
        y = asset_data[f"target_{timeframe}h"]

        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Train a simple model (e.g., RandomForestRegressor)
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # Compute performance metrics
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        rmse = mean_squared_error(y_test, y_pred) ** 0.5


        results.append({
            "timeframe": timeframe,
            "mae": mae,
            "mse": mse,
            "r2": r2,
            "rmse": rmse
        })

    # Find the best timeframe based on the lowest RMSE
    best_timeframe = min(results, key=lambda x: x["rmse"])

    return best_timeframe

# 🟢 Step 5: Apply Function to Each Asset and Store Results
optimal_timeframes = []

for asset, group in df.groupby("asset"):
    best_timeframe = get_optimal_timeframe(group)
    best_timeframe["asset"] = asset
    optimal_timeframes.append(best_timeframe)

# Convert results to DataFrame
df_optimal_timeframes = pd.DataFrame(optimal_timeframes)

# 🛠 Step 6: Upload Optimal Timeframes to BigQuery
to_gbq(df_optimal_timeframes, OPTIMAL_TIMEFRAMES_TABLE, project_id=PROJECT_ID, if_exists="replace")

print("✅ Optimal timeframes computed & uploaded to BigQuery!")
