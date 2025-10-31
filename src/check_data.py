import pandas as pd
import os

print("="*60)
print("DATA DIAGNOSTIC CHECK")
print("="*60)

# Check if processed data exists
if os.path.exists('processed_icu_data.csv'):
    print("\n✓ Found processed_icu_data.csv")
    
    # Load and inspect
    df = pd.read_csv('processed_icu_data.csv')
    
    print(f"\nData shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    if 'icu_count' in df.columns:
        print(f"\nICU Count Analysis:")
        print(f"  Data type: {df['icu_count'].dtype}")
        print(f"  Min: {df['icu_count'].min()}")
        print(f"  Max: {df['icu_count'].max()}")
        print(f"  Mean: {df['icu_count'].mean():.2f}")
        print(f"  Unique values: {sorted(df['icu_count'].unique())[:20]}")
        
        # Check for decimals
        has_decimals = (df['icu_count'] % 1 != 0).any()
        if has_decimals:
            print("\n⚠️ WARNING: Found decimal values in ICU count!")
            print("This should be whole numbers (can't have 0.5 patients)")
        else:
            print("\n✓ All ICU counts are whole numbers (correct!)")
            
        print(f"\nFirst 10 ICU counts: {df['icu_count'].head(10).tolist()}")
    
else:
    print("\n❌ processed_icu_data.csv not found")
    print("Run: python src/data_loader.py first")

# Check MIMIC data
mimic_path = "./data/mimic-iv/icu/icustays.csv"
if os.path.exists(mimic_path):
    print(f"\n✓ Found MIMIC icustays.csv")
    df_icu = pd.read_csv(mimic_path, nrows=5)
    print(f"  Columns: {df_icu.columns.tolist()}")
else:
    print(f"\n❌ MIMIC data not found at {mimic_path}")