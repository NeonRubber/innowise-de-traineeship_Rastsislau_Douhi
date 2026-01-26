from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}

with DAG(
    "task_6_airline_etl_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["task_6"],
) as dag:
    load_raw_task = SnowflakeOperator(
        task_id="load_raw_data",
        snowflake_conn_id="snowflake_conn",
        sql="CALL UTILS.LOAD_RAW_AIRLINE_DATA();",
        warehouse="COMPUTE_WH",
        database="AIRLINE_DB",
        schema="UTILS",
    )

    load_core_task = SnowflakeOperator(
        task_id="load_core_data",
        snowflake_conn_id="snowflake_conn",
        sql="CALL UTILS.LOAD_CORE_FLIGHTS();",
        warehouse="COMPUTE_WH",
        database="AIRLINE_DB",
        schema="UTILS",
    )

    load_analytics_task = SnowflakeOperator(
        task_id="load_analytics_data",
        snowflake_conn_id="snowflake_conn",
        sql="CALL UTILS.LOAD_ANALYTICS_STATS();",
        warehouse="COMPUTE_WH",
        database="AIRLINE_DB",
        schema="UTILS",
    )

    load_raw_task >> load_core_task >> load_analytics_task
