"""
Convert parquet files to CSV for ARM64 compatibility
"""
import sys
import pyarrow.parquet as pq
import pandas as pd

def convert_features_parquet():
    """Convert features.parquet to features.csv"""
    print("Reading features.parquet...")
    df = pd.read_parquet("data/processed/features.parquet")
    print(f"Read {len(df)} rows, {len(df.columns)} columns")
    
    print("Writing features.csv...")
    df.to_csv("data/processed/features.csv", index=False)
    print(f"✅ Converted to features.csv")
    
if __name__ == "__main__":
    convert_features_parquet()
