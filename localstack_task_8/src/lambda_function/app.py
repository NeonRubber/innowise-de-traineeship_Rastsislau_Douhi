import json
import logging
import os
import boto3
import pandas as pd
import numpy as np
from urllib.parse import unquote_plus
from adapters.s3_adapter import S3Adapter
from adapters.dynamo_adapter import DynamoDBAdapter

# Configuration
LOCALSTACK_ENDPOINT = os.environ.get('AWS_ENDPOINT_URL', 'http://localstack:4566')
TABLE_NAME = os.environ.get('TABLE_NAME', 'BikeMetrics')

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize adapters
s3_adapter = S3Adapter(endpoint_url=LOCALSTACK_ENDPOINT)
dynamo_adapter = DynamoDBAdapter(table_name=TABLE_NAME, endpoint_url=LOCALSTACK_ENDPOINT)

def lambda_handler(event, context):
    logger.info("Lambda started processing")
    
    for record in event.get('Records', []):
        try:
            # Parse Message
            body = json.loads(record['body'])
            if 'Message' in body:
                message = json.loads(body['Message'])
            else:
                message = body
                
            if 'Records' not in message:
                logger.warning("No S3 records found in message")
                continue
                
            for s3_record in message['Records']:
                bucket_name = s3_record['s3']['bucket']['name']
                key = unquote_plus(s3_record['s3']['object']['key'])
                
                logger.info(f"Processing event for s3://{bucket_name}/{key}")
                
                # Identify Month and Type
                month = None
                file_type = None
                
                if 'monthly/' in key:
                    month = key.split('/')[-1].replace('.csv', '')
                    file_type = 'raw'
                elif 'metrics/' in key:
                    month = key.split('/')[-1].replace('.csv', '')
                    file_type = 'processed'
                else:
                    logger.info(f"Skipping unknown file format: {key}")
                    continue
                
                # Log file receipt status in DynamoDB
                state_pk = f"STATE#{month}"
                dynamo_adapter.put_item({
                    'date': state_pk,
                    'metric_type': file_type,
                    'bucket': bucket_name,
                    'key': key,
                    'status': 'received'
                })
                
                # Proceed only if both raw and processed files are present
                raw_state = dynamo_adapter.get_item({'date': state_pk, 'metric_type': 'raw'})
                processed_state = dynamo_adapter.get_item({'date': state_pk, 'metric_type': 'processed'})
                
                if raw_state and processed_state:
                    logger.info(f"Both files received for {month}. Starting processing.")
                    
                    # Process the Spark-generated metrics file from S3
                    df = s3_adapter.read_csv(processed_state['bucket'], processed_state['key'])
                    logger.info(f"CSV Columns found: {list(df.columns)}")
                    
                    # Clean column names (strip spaces)
                    df.columns = df.columns.str.strip()
                    
                    # Sanitize data for DynamoDB compatibility (string-safe floats)
                    df = df.astype(object)
                    df = df.where(pd.notnull(df), None)
                    
                    metrics_to_save = df.to_dict('records')
                    
                    if not metrics_to_save:
                        logger.warning("Processed file is empty!")
                        continue

                    # Initialize DynamoDB resource for batch operations
                    dynamodb_resource = boto3.resource('dynamodb', endpoint_url=LOCALSTACK_ENDPOINT)
                    table = dynamodb_resource.Table(TABLE_NAME)

                    # Persist overall monthly aggregate summary
                    first = metrics_to_save[0]
                    monthly_stats = {
                        'date': f"STATS#{month}",
                        'metric_type': "OVERALL",
                        # Mapping with defaults for schema stability
                        'avg_distance': str(first.get('avg_distance_val', 0)),
                        'avg_duration': str(first.get('avg_duration_val', 0)),
                        'avg_speed': str(first.get('avg_speed_val', 0)),
                        'avg_temperature': str(first.get('avg_temp_val', 0)),
                        'total_stations_active': str(len(metrics_to_save))
                    }
                    table.put_item(Item=monthly_stats)
                    logger.info("Saved monthly overall stats")

                    # Persist individual station metrics via batch write
                    logger.info(f"Starting batch write for {len(metrics_to_save)} stations...")
                    
                    with table.batch_writer() as batch:
                        for item in metrics_to_save:
                            station_name = item.get('station_name', 'Unknown')
                            
                            # Prepare and insert item into batch
                            dynamo_item = {
                                'date': f"STATS#{month}",
                                'metric_type': f"STATION#{station_name}",
                                'departure_count': str(item.get('departure_count', 0)),
                                'avg_distance': str(item.get('avg_distance_val', 0)),
                                'avg_duration': str(item.get('avg_duration_val', 0)),
                                'avg_speed': str(item.get('avg_speed_val', 0)),
                                'avg_temperature': str(item.get('avg_temp_val', 0))
                            }
                            batch.put_item(Item=dynamo_item)
                    
                    logger.info(f"Successfully batch-wrote {len(metrics_to_save)} items.")
                
                else:
                    logger.info(f"Waiting for other file for {month}. Raw: {bool(raw_state)}, Processed: {bool(processed_state)}")
                    
        except Exception as e:
            logger.error(f"Failed to process record: {e}", exc_info=True)
            raise

    return {"statusCode": 200, "body": "Processing Complete"}