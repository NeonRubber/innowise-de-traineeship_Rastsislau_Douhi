# DynamoDB Adapter for Lambda database operations

import boto3
from decimal import Decimal
import json
import logging

logger = logging.getLogger()

class DynamoDBAdapter:
    def __init__(self, table_name: str, endpoint_url=None, region_name='us-east-1'):
        self.dynamodb = boto3.resource(
            'dynamodb', 
            endpoint_url=endpoint_url,
            region_name=region_name
        )
        self.table = self.dynamodb.Table(table_name)

    def _float_to_decimal(self, data):
        # Recursively convert floats to Decimals for DynamoDB compatibility
        if isinstance(data, float):
            return Decimal(str(data))
        elif isinstance(data, dict):
            return {k: self._float_to_decimal(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._float_to_decimal(v) for v in data]
        return data

    def put_item(self, item: dict):
        try:
            # Sanitize item and persist to DynamoDB
            db_item = self._float_to_decimal(item)
            self.table.put_item(Item=db_item)
            logger.info(f"Saved item to DynamoDB: {item.get('date')} - {item.get('metric_type')}")
        except Exception as e:
            logger.error(f"Error saving to DynamoDB: {e}")
            raise

    def get_item(self, key: dict):
        try:
            response = self.table.get_item(Key=key)
            return response.get('Item')
        except Exception as e:
            logger.error(f"Error getting item from DynamoDB: {e}")
            raise
