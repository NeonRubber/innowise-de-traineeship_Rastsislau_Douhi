# DAG to orchestrate the monthly bike data processing pipeline
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import boto3

# Custom utilities and path configuration
import sys
sys.path.insert(0, '/opt/airflow/src')
from common.aws_utils import create_boto3_client, upload_file_to_s3

def download_for_tableau(**context):
    # LocalStack S3 configuration
    ENDPOINT_URL = 'http://localstack:4566'
    BUCKET_NAME = 'processed-metrics'
    
    # Define the source file key in S3
    FILE_KEY = 'metrics/2020-10.csv' 
    
    # Local destination path within the container (mapped to Windows host)
    LOCAL_PATH = '/opt/airflow/dags/tableau_data/final_dataset.csv'
    
    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    
    print(f"Downloading {BUCKET_NAME}/{FILE_KEY} to {LOCAL_PATH}...")
    
    # S3 client initialization
    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    # Download file from S3 to local path
    try:
        s3.download_file(BUCKET_NAME, FILE_KEY, LOCAL_PATH)
        print("Download successful!")
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Paths configuration
# Container directory paths
DATA_DIR = '/opt/airflow/data/monthly'
SPARK_JOB_PATH = '/opt/airflow/src/spark_jobs/calculate_stations.py'
OUTPUT_BASE_DIR = '/opt/airflow/data/output'

# AWS Configuration
S3_RAW_BUCKET = 'raw-data'
S3_PROCESSED_BUCKET = 'processed-metrics'

# Python Functions

def get_latest_monthly_file() -> str:
    # Get the latest monthly CSV file from data directory
    data_path = Path(DATA_DIR)
    csv_files = list(data_path.glob('*.csv'))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")
    
    latest_file = sorted(csv_files)[-1]
    return str(latest_file)

def upload_raw_to_s3(**context):
    # Find the latest available monthly CSV file
    file_path = get_latest_monthly_file()
    filename = Path(file_path).name
    s3_key = f"monthly/{filename}"
    
    print(f"Uploading {file_path} to s3://{S3_RAW_BUCKET}/{s3_key}")
    
    s3_client = create_boto3_client('s3')
    s3_uri = upload_file_to_s3(
        file_path=file_path,
        bucket=S3_RAW_BUCKET,
        key=s3_key,
        s3_client=s3_client
    )
    
    print(f"Successfully uploaded to {s3_uri}")
    
    context['task_instance'].xcom_push(key='raw_s3_key', value=s3_key)
    context['task_instance'].xcom_push(key='filename', value=filename)
    
    return s3_uri

def run_spark_job(**context):
    filename = context['task_instance'].xcom_pull(
        task_ids='upload_raw_to_s3',
        key='filename'
    )
    
    if not filename:
        raise ValueError("Filename not found in XCom")
    
    input_path = os.path.join(DATA_DIR, filename)
    month_name = Path(filename).stem
    output_path = os.path.join(OUTPUT_BASE_DIR, f"metrics_{month_name}")
    
    print(f"Running Spark job:")
    print(f"  Input: {input_path}")
    print(f"  Output: {output_path}")
    
    command = [
        'spark-submit',
        '--master', 'local[*]',
        '--driver-memory', '2g',
        '--executor-memory', '2g',
        SPARK_JOB_PATH,
        input_path,
        output_path
    ]
    
    print(f"Command: {' '.join(command)}")
    
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True
        )
        print("Spark job stdout:")
        print(result.stdout)
        
        context['task_instance'].xcom_push(key='output_path', value=output_path)
        context['task_instance'].xcom_push(key='month_name', value=month_name)
        
        return output_path
        
    except subprocess.CalledProcessError as e:
        print(f"Spark job failed with exit code {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise

def upload_processed_to_s3(**context):
    output_path = context['task_instance'].xcom_pull(
        task_ids='run_spark_job',
        key='output_path'
    )
    month_name = context['task_instance'].xcom_pull(
        task_ids='run_spark_job',
        key='month_name'
    )
    
    if not output_path or not month_name:
        raise ValueError("Output path or month name not found in XCom")
    
    output_dir = Path(output_path)
    # Search for Spark output CSV part files within the directory
    csv_files = list(output_dir.glob('**/*.csv'))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {output_path}")
    
    csv_file = csv_files[0]
    s3_key = f"metrics/{month_name}.csv"
    
    print(f"Uploading {csv_file} to s3://{S3_PROCESSED_BUCKET}/{s3_key}")
    
    s3_client = create_boto3_client('s3')
    s3_uri = upload_file_to_s3(
        file_path=str(csv_file),
        bucket=S3_PROCESSED_BUCKET,
        key=s3_key,
        s3_client=s3_client
    )
    
    print(f"Successfully uploaded to {s3_uri}")
    return s3_uri

# DAG Definition

with DAG(
    dag_id='bike_processing_pipeline',
    default_args=DEFAULT_ARGS,
    description='Process monthly bike data and calculate station metrics',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['bikes', 'etl', 'spark'],
) as dag:
    
    # Task 1: Automatically split raw data
    run_splitter = BashOperator(
        task_id='run_data_splitter',
        bash_command='python /opt/airflow/scripts/data_splitter.py',
        env={
            # Raw data input path within the container (ensure it is named "bikes.csv")
            'CSV_INPUT_PATH': '/opt/airflow/data/bikes.csv',
            
            'MONTHLY_OUTPUT_DIR': '/opt/airflow/data/monthly',
            
            'DATE_COLUMN': 'departure',
            
            'CHUNK_SIZE': '500000'
        }
    )
    
    # Task 2: Upload raw file to S3
    upload_raw = PythonOperator(
        task_id='upload_raw_to_s3',
        python_callable=upload_raw_to_s3,
        provide_context=True,
    )
    
    # Task 3: Run Spark job
    spark_job = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
        provide_context=True,
    )
    
    # Task 4: Upload processed results to S3
    upload_processed = PythonOperator(
        task_id='upload_processed_to_s3',
        python_callable=upload_processed_to_s3,
        provide_context=True,
    )

    # Task 5: Save data for Tableau
    save_to_local_task = PythonOperator(
        task_id='save_data_for_tableau',
        python_callable=download_for_tableau,
        provide_context=True
    )
    
    # Define task dependencies
    run_splitter >> upload_raw >> spark_job >> upload_processed >> save_to_local_task