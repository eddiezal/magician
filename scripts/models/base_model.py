import os
import pickle
import pandas as pd
import numpy as np
from google.cloud import bigquery
from sklearn.metrics import accuracy_score, classification_report

class BaseModel:
    """
    👑 **BaseModel – The Godfather of All Models**
    
    This class:
    - Trains models (so you don’t have to do it manually like a peasant)
    - Evaluates performance (because guessing is not a strategy)
    - Logs everything to BigQuery (so future you doesn’t hate past you)
    - Saves models (because recreating models from scratch is for amateurs)
    
    Future extensions? Sure. But for now, this is the **organized chaos** we need.
    """

    def __init__(self, model, model_name="BaseModel"):
        self.model = model
        self.model_name = model_name
        self.client = bigquery.Client()

    def fit(self, X_train, y_train):
        """🚀 Train the model, because models don’t train themselves (yet)."""
        print(f"🛠 Training {self.model_name}...")
        self.model.fit(X_train, y_train)

    def evaluate(self, X_test, y_test):
        """📊 Evaluates the model and logs metrics to BigQuery like a responsible adult."""
        print(f"🔍 Evaluating {self.model_name}...")
        predictions = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        report = classification_report(y_test, predictions, output_dict=True)
        
        print(f"🎯 Accuracy: {accuracy:.4f}")
        
        # 🚀 Log to BigQuery
        self.log_results(accuracy, report)
        return report

    def log_results(self, accuracy, report):
        """📝 Logs model performance to BigQuery (so we can remember what worked)."""
        table_id = "cloud4marketing-281206.crypto_models.performance_logs"
        data = {"model_name": self.model_name, "accuracy": accuracy}
        df = pd.DataFrame([data])
        
        df.to_gbq(table_id, project_id="cloud4marketing-281206", if_exists="append")
        print("✅ Results logged to BigQuery!")

    def save_model(self):
        """💾 Saves the trained model like a digital horcrux."""
        filename = f"models/{self.model_name}.pkl"
        with open(filename, "wb") as f:
            pickle.dump(self.model, f)
        print(f"📁 Model saved as {filename}")

