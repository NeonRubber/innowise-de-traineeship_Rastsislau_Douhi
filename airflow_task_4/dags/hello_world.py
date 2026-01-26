from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {"start_date": datetime(2025, 12, 15)}


@dag(default_args=default_args, schedule=None, catchup=False)
def hello_world():
    BashOperator(task_id="hello_world_dag", bash_command="echo 'Hello world!'")


hello_world()
