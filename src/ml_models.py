import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import joblib
import matplotlib.pyplot as plt

def prepare_features(df):
    """Prepare features for ML"""
    # Create lag features (previous values)
    df['icu_count_lag1'] = df['icu_count'].shift(1)
    df['icu_count_lag2'] = df['icu_count'].shift(2)
    
    # Create target (next hour's count)
    df['target'] = df['icu_count'].shift(-1)
    
    # Remove rows with missing values
    df = df.dropna()
    
    return df

def train_model(df):
    """Train a Random Forest model"""
    # Define features and target
    feature_cols = ['hour', 'day_of_week', 'icu_count', 
                   'icu_count_lag1', 'icu_count_lag2']
    X = df[feature_cols]
    y = df['target']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train model
    print("Training Random Forest model...")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Make predictions
    predictions = model.predict(X_test)
    
    # Calculate error
    mae = mean_absolute_error(y_test, predictions)
    print(f"Mean Absolute Error: {mae:.2f}")
    
    # Save model
    joblib.dump(model, 'icu_model.pkl')
    print("Model saved to 'icu_model.pkl'")
    
    # Plot results
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    plt.scatter(y_test, predictions, alpha=0.5)
    plt.xlabel('Actual ICU Count')
    plt.ylabel('Predicted ICU Count')
    plt.title('Predictions vs Actual')
    
    plt.subplot(1, 2, 2)
    plt.plot(y_test.values[:50], label='Actual', marker='o')
    plt.plot(predictions[:50], label='Predicted', marker='s')
    plt.xlabel('Time')
    plt.ylabel('ICU Count')
    plt.title('First 50 Predictions')
    plt.legend()
    
    plt.tight_layout()
    plt.savefig('model_results.png')
    print("Results saved to 'model_results.png'")
    plt.show()
    
    return model

if __name__ == "__main__":
    print("="*50)
    print("Training ICU Forecasting Model")
    print("="*50)
    
    # Load processed data
    df = pd.read_csv('processed_icu_data.csv')
    print(f"Loaded data: {df.shape}")
    
    # Prepare features
    df = prepare_features(df)
    
    # Train model
    model = train_model(df)