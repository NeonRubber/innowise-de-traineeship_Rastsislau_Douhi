from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

# Аргументы по умолчанию - применяются ко всем задачам в DAG'е
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

FILE_PATH = "/opt/airflow/data/tiktok_google_play_reviews.csv"

with DAG(
    dag_id="data_sensor",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    wait_for_file_task = FileSensor(
        task_id="waiting_for_dataset",
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=600,
        mode="poke",
    )
