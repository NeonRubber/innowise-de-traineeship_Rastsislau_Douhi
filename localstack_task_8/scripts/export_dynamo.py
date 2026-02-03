# Export DynamoDB Data to CSV for Tableau

import boto3
import pandas as pd
import os
from decimal import Decimal

# Configuration
LOCALSTACK_ENDPOINT = os.getenv('LOCALSTACK_ENDPOINT', 'http://localhost:4566')
TABLE_NAME = 'BikeMetrics'
OUTPUT_FILE = 'final_dashboard_data.csv'


def get_dynamodb_resource():
    # Connect to DynamoDB on LocalStack
    return boto3.resource(
        'dynamodb',
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )


def convert_decimal(obj):
    # Helper to convert Decimal types to float/int for DataFrame compatibility
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def export_data():
    print(f"Connecting to DynamoDB at {LOCALSTACK_ENDPOINT}...")
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(TABLE_NAME)

    try:
        print(f"Scanning table '{TABLE_NAME}'...")
        # Scan the table (retrieving all items)
        response = table.scan()
        items = response.get('Items', [])
        
        # Handle pagination if table is large
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        print(f"Retrieved {len(items)} items.")

        if not items:
            print("No data found in DynamoDB.")
            return

        # Convert list of dicts to DataFrame
        # DynamoDB often returns Decimals for numbers, which pandas/Tableau might not like.
        # We'll convert them to floats.
        clean_items = []
        for item in items:
            clean_item = {k: convert_decimal(v) for k, v in item.items()}
            clean_items.append(clean_item)

        df = pd.DataFrame(clean_items)

        # Ensure numeric columns are actually numeric
        # Assuming typical metrics might be 'trip_count', 'duration', etc.
        # We convert all columns that look numeric to numeric types.
        for col in df.columns:
            pd.to_numeric(df[col], errors='ignore')

        print(f"Saving to {OUTPUT_FILE}...")
        df.to_csv(OUTPUT_FILE, index=False)
        print("Export complete!")
        print(df.head())

    except Exception as e:
        print(f"Error exporting data: {e}")


if __name__ == '__main__':
    export_data()
