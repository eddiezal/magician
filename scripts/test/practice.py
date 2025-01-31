from scripts.indicators.compute_indicators import get_optimal_timeframe
from sklearn.model_selection import train_test_split

print("🔍 Running quick test...")
df = get_optimal_timeframe()
print(df.head())  # See if we get meaningful output
