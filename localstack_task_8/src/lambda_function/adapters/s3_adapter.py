# S3 Adapter for Lambda file operations

import boto3
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger()

class S3Adapter:
    def __init__(self, endpoint_url=None):
        self.s3 = boto3.client('s3', endpoint_url=endpoint_url)

    def read_csv(self, bucket: str, key: str) -> pd.DataFrame:
        try:
            logger.info(f"Reading file from s3://{bucket}/{key}")
            response = self.s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            return pd.read_csv(BytesIO(content))
        except Exception as e:
            logger.error(f"Error reading S3 file: {e}")
            raise
