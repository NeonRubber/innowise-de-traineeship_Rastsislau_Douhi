import pandas as pd
from pathlib import Path
import sys
import os

# Container directory paths
BASE_DIR = Path('/opt/airflow/data')
INPUT_FILE = BASE_DIR / 'bikes.csv'  
OUTPUT_DIR = BASE_DIR / 'monthly'

DATE_COL = 'departure' 

def split_data():
    print(f"--- Starting Simple Splitter ---")
    print(f"Looking for file: {INPUT_FILE}")
    print(f"Target Date Column: {DATE_COL}")
    
    if not INPUT_FILE.exists():
        print(f"CRITICAL ERROR: File not found at {INPUT_FILE}")
        # Log directory contents for debugging if file is missing
        try:
            print(f"Directory contents: {os.listdir(BASE_DIR)}")
        except:
            pass
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Process file in chunks to optimize memory usage
    chunk_size = 500000
    print(f"Reading {INPUT_FILE} in chunks...")

    try:
        # parse_dates=['departure']
        with pd.read_csv(INPUT_FILE, chunksize=chunk_size, parse_dates=[DATE_COL]) as reader:
            for i, chunk in enumerate(reader):
                print(f"Processing chunk {i}...")
                
                # Extract month for partitioning from the date column
                chunk['temp_month'] = chunk[DATE_COL].dt.to_period('M').astype(str)
                
                for month in chunk['temp_month'].unique():
                    month_data = chunk[chunk['temp_month'] == month].copy()
                    month_data = month_data.drop(columns=['temp_month'])
                    
                    output_file = OUTPUT_DIR / f"{month}.csv"
                    
                    # Append to existing file or create new one with header
                    mode = 'a' if output_file.exists() else 'w'
                    header = not output_file.exists()
                    
                    month_data.to_csv(output_file, mode=mode, header=header, index=False)
                    
        print("Success!")

    except Exception as e:
        print(f"ERROR: {e}")
        # Raise an error if it is related to date issues
        if "KeyError" in str(e) or "not in index" in str(e):
            print(f"HINT: Check if column '{DATE_COL}' actually exists in the CSV header.")
        sys.exit(1)

if __name__ == "__main__":
    split_data()