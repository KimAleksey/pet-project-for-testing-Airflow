from airflow import DAG
from airflow.operators.empty import EmptyOperator

from pendulum import datetime, duration

from utils.group_tasks_for_logging_dates import group_tasks_for_logging_dates

# Конфигурация DAG
OWNER = "kim-av"
DAG_ID = "dag_with_groups"
TAGS = ["example", "group"]

LONG_DESCRIPTION = """
Пример реализации DAG с Group tasks.
https://airflow.apache.org/docs/apache-airflow/2.0.1/_modules/airflow/example_dags/example_task_group.html
"""

SHORT_DESCRIPTION = "Пример реализации DAG с Group tasks."

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 1),
    catchup=True,
    default_args=default_args,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    start_task = EmptyOperator(task_id="start_task", dag=dag)

    group_task = group_tasks_for_logging_dates("group_of_tasks_1")

    end_task = EmptyOperator(task_id="end_task", dag=dag)

    start_task >> group_task >> end_task