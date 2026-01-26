from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago
import pandas as pd

PROCESSED_DATASET = Dataset("file://opt/airflow/data/tiktok_google_play_reviews.csv")
FILE_PATH = "/opt/airflow/data/tiktok_google_play_reviews.csv"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


def _load_to_mongo():
    print("Reading CSV file...")
    df = pd.read_csv(FILE_PATH)

    print("Converting data to a dictionary...")
    data = df.to_dict(orient="records")

    print(f"Connecting to MongoDB... Ready to insert {len(data)} records.")

    hook = MongoHook(conn_id="mongo_default")

    hook.insert_many(
        mongo_collection="tiktok_google_play_reviews", docs=data, mongo_db="airflow_db"
    )
    print("Data insertion is successful!")


with DAG(
    dag_id="load_data_to_mongo",
    default_args=default_args,
    # Data-aware scheduling - DAG сработает только после внесения изменений в файл
    schedule=[PROCESSED_DATASET],
    catchup=False,
) as dag:
    load_task = PythonOperator(
        task_id="load_csv_contents_to_mongo", python_callable=_load_to_mongo
    )
