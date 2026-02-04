import pandas as pd
import numpy as np
import os

def generate_loads(input_file='superstore.csv'):
    # Check root and data/ folder
    if not os.path.exists(input_file):
        if os.path.exists(os.path.join('data', input_file)):
            input_file = os.path.join('data', input_file)
        else:
            print(f"Error: {input_file} not found in root or data/ folder.")
            return

    # Load dataset
    df = pd.read_csv(input_file, encoding='windows-1252') # Common encoding for Superstore CSV
    
    # Ensure Order Date is datetime
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    df = df.sort_values('Order Date')

    # 1. Split logic: 80% initial, 20% secondary
    split_idx = int(len(df) * 0.8)
    initial_load = df.iloc[:split_idx].copy()
    secondary_load = df.iloc[split_idx:].copy()

    # 2. Duplicates: 5 random rows from initial_load
    duplicates = initial_load.sample(n=5, random_state=42)
    secondary_load = pd.concat([secondary_load, duplicates])

    # 3. SCD Type 1 (Correction): Fix typo in name for 1 customer
    # Pick a customer who exists in both
    common_customers = set(initial_load['Customer ID']).intersection(set(secondary_load['Customer ID']))
    if common_customers:
        scd1_cust_id = list(common_customers)[0]
        # In secondary load, change the name slightly
        secondary_load.loc[secondary_load['Customer ID'] == scd1_cust_id, 'Customer Name'] += " (Corrected)"
        print(f"Applied SCD Type 1 for Customer ID: {scd1_cust_id}")

    # 4. SCD Type 2 (History): Change Region/Segment for 1 customer
    # Pick another customer from common ones
    if len(common_customers) > 1:
        scd2_cust_id = list(common_customers)[1]
        # In secondary load, change Region
        secondary_load.loc[secondary_load['Customer ID'] == scd2_cust_id, 'Region'] = 'New Region'
        print(f"Applied SCD Type 2 for Customer ID: {scd2_cust_id}")

    # Save files
    initial_load.to_csv('data/initial_load.csv', index=False)
    secondary_load.to_csv('data/secondary_load.csv', index=False)
    print("Successfully generated data/initial_load.csv and data/secondary_load.csv")

if __name__ == "__main__":
    generate_loads()
