# magician

## New Feature: Optimal N-hour Selection Per Asset

### Overview

This feature analyzes past price movements to determine the best timeframe per asset. Different assets move at different speeds, and this feature helps in selecting the optimal prediction window for each asset.

### Steps

1. **Pull Historical Price Data**: Fetch historical price data from BigQuery.
2. **Test Multiple Prediction Windows**: Test multiple prediction windows (1h, 2h, 4h, etc.).
3. **Compute Predictive Performance**: Compute which timeframe gives the best predictive performance.
4. **Store Results in BigQuery**: Store the results dynamically in BigQuery.

### Predictive Performance Metrics

The following metrics are used to define predictive performance for different timeframes:

- **Mean Absolute Error (MAE)**: Measures the average magnitude of errors in predictions.
- **Mean Squared Error (MSE)**: Measures the average of the squares of the errors.
- **R-squared (R²) Score**: Measures the proportion of the variance in the dependent variable that is predictable from the independent variables.
- **Root Mean Squared Error (RMSE)**: The square root of the average of squared differences between prediction and actual observation.
- **Accuracy**: For classification tasks, measures the proportion of correct predictions out of all predictions made.

### Usage

To use the `get_optimal_timeframe` function, follow these steps:

1. **Import the Function**: Import the `get_optimal_timeframe` function from `scripts/indicators/compute_indicators`.
2. **Fetch Optimal Timeframes**: Use the `get_optimal_timeframe` function to fetch the optimal timeframes for each asset.
3. **Store Results**: The results are stored dynamically in BigQuery under a new table `optimal_timeframes`.

### Example

```python
from scripts.indicators.compute_indicators import get_optimal_timeframe

# Fetch historical price data
df = fetch_historical_data()

# Get optimal timeframes for each asset
optimal_timeframes = get_optimal_timeframe(df)

# Print the results
print(optimal_timeframes)
```

### BigQuery Integration

The results are stored dynamically in BigQuery. Ensure that the Google Cloud credentials are correctly set up and the BigQuery client is initialized.

### Handling Different Asset Types

The analysis handles different asset types by fetching data for each asset, grouping the data by asset, computing technical indicators, and storing the results dynamically in BigQuery.

### Error Handling

Implement error handling to manage any issues that arise during the data processing and storage steps. This ensures that any problems are logged and can be addressed.
