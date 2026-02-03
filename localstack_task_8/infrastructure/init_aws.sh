#!/bin/bash

# Exit on error

set -e

echo "Initializing AWS Resources in LocalStack"

# Wait for LocalStack initialization
echo "Waiting for LocalStack..."
sleep 5

# S3 Buckets
echo "Creating S3 Buckets..."
awslocal s3 mb s3://raw-data || echo "Bucket raw-data exists"
awslocal s3 mb s3://processed-metrics || echo "Bucket processed-metrics exists"

# DynamoDB Table
echo "Creating DynamoDB Table..."
awslocal dynamodb create-table \
    --table-name BikeMetrics \
    --attribute-definitions \
        AttributeName=date,AttributeType=S \
        AttributeName=metric_type,AttributeType=S \
    --key-schema \
        AttributeName=date,KeyType=HASH \
        AttributeName=metric_type,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    2>/dev/null || echo "Table BikeMetrics exists"

# SNS Topic
echo "Creating SNS Topic..."
TOPIC_ARN=$(awslocal sns create-topic --name DataUploadTopic --query 'TopicArn' --output text)
echo "SNS ARN: $TOPIC_ARN"

# SQS Queue
echo "Creating SQS Queue..."
QUEUE_URL=$(awslocal sqs create-queue --queue-name DataProcessingQueue --query 'QueueUrl' --output text)
QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
echo "SQS ARN: $QUEUE_ARN"

# SNS and SQS Subscriptions
echo "Subscribing SQS to SNS..."
awslocal sns subscribe \
    --topic-arn "$TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$QUEUE_ARN" \
    > /dev/null

echo "Configuring S3 Notifications..."
CONFIG_JSON='{"TopicConfigurations": [{"TopicArn": "'"$TOPIC_ARN"'", "Events": ["s3:ObjectCreated:*"]}]}'
awslocal s3api put-bucket-notification-configuration --bucket raw-data --notification-configuration "$CONFIG_JSON"
awslocal s3api put-bucket-notification-configuration --bucket processed-metrics --notification-configuration "$CONFIG_JSON"

# Lambda Deployment (with Pandas installation)
echo "Deploying Lambda Function..."

# Prepare build directory for Lambda package
BUILD_DIR="/tmp/lambda_build"

# Clean and recreate build directory
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "Step 1: Copying source code..."
# Copy source code to build directory
cp -r /opt/code/lambda/. "$BUILD_DIR/"

echo "Step 2: Installing Pandas (This may take 1-2 minutes)..."
echo "Step 2: Installing Pandas (Specific Linux Binaries)..."
# Install optimized Pandas and Numpy for Lambda environment
pip install \
    --target "$BUILD_DIR" \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.10 \
    --only-binary=:all: \
    --upgrade \
    pandas==2.0.3 numpy==1.25.2

echo "Step 3: Zipping package..."
cd "$BUILD_DIR"
# Cleanup build artifacts to reduce package size
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name "*.dist-info" -exec rm -rf {} +
find . -type d -name "tests" -exec rm -rf {} +

# Create deployment archive
zip -r -q /tmp/function.zip .
cd - > /dev/null

echo "Step 4: Creating Function..."
# Deploy Lambda function with optimized configuration
awslocal lambda create-function \
    --function-name BikeProcessor \
    --runtime python3.10 \
    --zip-file fileb:///tmp/function.zip \
    --handler app.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --environment Variables="{AWS_ENDPOINT_URL=http://localstack:4566,TABLE_NAME=BikeMetrics}" \
    --timeout 60

# --- 8. Event Source Trigger ---
echo "Setting up SQS Trigger..."
awslocal lambda create-event-source-mapping \
    --function-name BikeProcessor \
    --batch-size 1 \
    --event-source-arn "$QUEUE_ARN"

echo "Initialization Complete!"