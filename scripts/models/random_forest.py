from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from base_model import BaseModel


class RandomForestModel(BaseModel):
    """
    🌲 Random Forest Model extending BaseModel.  
    Think of it as your **low-maintenance, high-output** employee.  
    No burnout, no excuses, just trees doing their job.  

    ✅ Supports:
        - Training (because raw data won’t trade itself)  
        - Predictions (hopefully better than your last impulse buy)  
        - Hyperparameter tuning (because ‘default settings’ are for amateurs)  
        - Model persistence (so you don’t have to retrain every time you sneeze)  
    """

    def __init__(self, n_estimators=100, max_depth=None):
        """
        Initializes the Random Forest model with settings that won’t make you regret your life choices.

        :param n_estimators: How many trees in the forest. More = better (usually).  
                            But don’t get greedy—too many trees, and your model moves slower than a boomer opening a new tab.  
        :param max_depth: How deep the trees go.  
                          Set it too high, and your model overfits faster than a trader maxing out leverage on a meme coin.  
        """
        super().__init__("random_forest")
        self.model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, random_state=42)

    def train(self, X_train, y_train):
        """
        Train the Random Forest model on your carefully chosen, definitely-not-biased data.

        :param X_train: Features for training  
        :param y_train: Target values  
        """
        if X_train is None or y_train is None:
            raise ValueError("🚨 Missing data! Even the best strategies need actual numbers to work with.")

        print("🚀 Training Random Forest Model... This might take longer than your morning coffee, but it’s worth it.")
        self.model.fit(X_train, y_train)
        self.save_model()
        print("✅ Model trained and saved! Let’s hope it doesn’t predict like a Magic 8-ball.")

    def predict(self, X):
        """
        Generate predictions. Because that’s literally the whole point.

        :param X: Input features  
        :return: Predictions (that may or may not be better than your gut feeling)  
        """
        if self.model:
            return self.model.predict(X)
        else:
            raise RuntimeError("⚠️ Model not trained. Even ‘diamond hands’ need a backtest.")

    def tune_hyperparameters(self, X_train, y_train):
        """
        Tune hyperparameters using GridSearchCV. Because one-size-fits-all doesn’t apply to trading.

        :param X_train: Features for training  
        :param y_train: Target values  
        """
        if X_train is None or y_train is None:
            raise ValueError("🚨 You can’t optimize what doesn’t exist. Feed the model some data.")

        print("🔍 Tuning Hyperparameters... because ‘set it and forget it’ is not a strategy.")

        param_grid = {
            "n_estimators": [50, 100, 200],
            "max_depth": [None, 10, 20],
            "min_samples_split": [2, 5, 10],
        }

        grid_search = GridSearchCV(
            RandomForestRegressor(random_state=42), 
            param_grid, 
            cv=3, 
            scoring="r2", 
            verbose=1, 
            n_jobs=-1
        )

        grid_search.fit(X_train, y_train)
        self.model = grid_search.best_estimator_
        self.save_model()

        print(f"✅ Best Parameters: {grid_search.best_params_} (Finally, something optimized better than your morning routine.)")
        print("✅ Model is locked and loaded! Now go make some trades.")
