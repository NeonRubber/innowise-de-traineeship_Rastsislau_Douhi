from airflow import DAG, Dataset
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import os
import pandas as pd
import re

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

FILE_PATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
PROCESSED_DATASET = Dataset("file://opt/airflow/data/tiktok_google_play_reviews.csv")

# Функция проверки файла (пустой/заполненный)
def _check_file_empty(**context):
    # Возвращает размер файла в байтах
    file_size = os.stat(FILE_PATH).st_size
    
    print(f"Размер файла: {file_size} байт")

    if file_size == 0:
        # Если файл пустой - задача логирования
        return 'file_is_empty_log'
    else:
        # Если с содержимым - задача обработки
        return 'data_processing.replace_nulls'
def _replace_nulls():
    print("Replacing nulls...")
    df = pd.read_csv(FILE_PATH)
    df = df.replace('null', '-')
    df = df.fillna('-')
    # Переименовываем колонку по ТЗ
    if 'at' in df.columns:
        df.rename(columns={'at': 'created_date'}, inplace=True)
    df.to_csv(FILE_PATH, index=False)

def _sort_by_date():
    print("Sorting by date...")
    df = pd.read_csv(FILE_PATH)
    if 'created_date' in df.columns:
        df['created_date'] = pd.to_datetime(df['created_date'])
        df = df.sort_values(by='created_date')
    df.to_csv(FILE_PATH, index=False)

def _content_column_cleaning():
    print("Cleaning content...")
    df = pd.read_csv(FILE_PATH)
    def clean_text(text):
        if not isinstance(text, str):
            return str(text)
        return re.sub(r'[^\w\s\.,!?-]', '', text)
    
    if 'content' in df.columns:
        df['content'] = df['content'].apply(clean_text)
    df.to_csv(FILE_PATH, index=False)

with DAG(
    dag_id='data_process',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Сенсор
    wait_for_file = FileSensor(
        task_id='waiting_for_dataset',
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=600,
        mode='poke'
    )

    # Ветвление
    check_empty = BranchPythonOperator(
        task_id='check_if_file_is_empty',
        python_callable=_check_file_empty
    )

    # При пустом файле - логирование через Bash
    log_empty = BashOperator(
        task_id='file_is_empty_log',
        bash_command='echo "The file is empty! Processing has been stopped."'
    )

    # При заполненном файле - обработка
    with TaskGroup("data_processing") as data_processing:

        replace_nulls = PythonOperator(
            task_id='replace_nulls',
            python_callable=_replace_nulls
        )

        sort_by_date = PythonOperator(
            task_id='sort_by_date',
            python_callable=_sort_by_date
        )

        content_column_cleaning = PythonOperator(
            task_id='content_column_cleaning',
            python_callable=_content_column_cleaning,
            outlets=[PROCESSED_DATASET]
        )

        replace_nulls >> sort_by_date >> content_column_cleaning

    # Последовательность задач
    wait_for_file >> check_empty
    check_empty >> log_empty
    check_empty >> data_processing