import os
import json
import pandas as pd
from google.cloud import bigquery
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from load_data import load_data  # Load the freshest crypto data
from models.base_model import BaseModel  # Our base class for all models
from models.random_forest import RandomForestModel  # Import models dynamically later?

# BigQuery Configuration
PROJECT_ID = "cloud4marketing-281206"
DATASET_ID = "crypto_price"
MODEL_RESULTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.model_results"

# 🚀 Step 1: Load Data
print("📡 Fetching and splitting data... because we like informed decisions.")
X_train, X_test, y_train, y_test = load_data()

# 🚀 Step 2: Select Model
MODEL_MAPPING = {
    "random_forest": RandomForestModel,
    # Future expansion: "lstm": LSTMModel, "xgboost": XGBoostModel
}

model_name = os.getenv("MODEL_NAME", "random_forest")  # Default: Random Forest
if model_name not in MODEL_MAPPING:
    raise ValueError(f"🤨 Unknown model '{model_name}'. Supported models: {list(MODEL_MAPPING.keys())}")

ModelClass = MODEL_MAPPING[model_name]
model = ModelClass()

# 🚀 Step 3: Train Model
print(f"🎯 Training `{model_name}` model... hope it doesn't disappoint.")
model.train(X_train, y_train)

# 🚀 Step 4: Make Predictions
y_pred = model.predict(X_test)

# 🚀 Step 5: Evaluate Performance
print("📊 Evaluating model performance... because data-driven gloating is the best kind.")
metrics = {
    "model": model_name,
    "r2_score": r2_score(y_test, y_pred),
    "mae": mean_absolute_error(y_test, y_pred),
    "mse": mean_squared_error(y_test, y_pred),
    "rmse": mean_squared_error(y_test, y_pred, squared=False),
    "feature_importances": model.get_feature_importance().to_dict() if hasattr(model, "get_feature_importance") else "N/A",
}

print(json.dumps(metrics, indent=4))

# 🚀 Step 6: Save Model (Because We Ain’t Training This Twice!)
model.save(f"{model_name}_model.pkl")

# 🚀 Step 7: Store Results in BigQuery
client = bigquery.Client()
df_metrics = pd.DataFrame([metrics])
df_metrics["timestamp"] = pd.Timestamp.utcnow()

print("📡 Uploading model results to BigQuery... because logs are life.")
df_metrics.to_gbq(MODEL_RESULTS_TABLE, project_id=PROJECT_ID, if_exists="append")

print(f"✅ Training complete! `{model_name}` results saved locally & in BigQuery! 🚀")
