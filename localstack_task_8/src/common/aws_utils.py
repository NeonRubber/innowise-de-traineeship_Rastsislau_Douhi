# AWS Utilities for LocalStack integration

import os
from typing import Optional
import boto3
from botocore.client import BaseClient


def get_localstack_endpoint() -> str:
    # Get the LocalStack endpoint URL
    return os.getenv('LOCALSTACK_ENDPOINT', 'http://localstack:4566')


def create_boto3_client(
    service_name: str,
    endpoint_url: Optional[str] = None,
    region_name: str = 'us-east-1',
    aws_access_key_id: str = 'test',
    aws_secret_access_key: str = 'test'
) -> BaseClient:
    # Create a boto3 client configured for LocalStack
    if endpoint_url is None:
        endpoint_url = get_localstack_endpoint()
    
    return boto3.client(
        service_name,
        endpoint_url=endpoint_url,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def upload_file_to_s3(
    file_path: str,
    bucket: str,
    key: str,
    s3_client: Optional[BaseClient] = None
) -> str:
    # Upload a file to S3 bucket
    if s3_client is None:
        s3_client = create_boto3_client('s3')
    
    s3_client.upload_file(file_path, bucket, key)
    return f"s3://{bucket}/{key}"
