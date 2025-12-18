from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from pendulum import duration, datetime

import logging

def log_value(value, **context):
    logging.info(f"Value: {value}")

# Конфигурация DAG
OWNER = "kim-av"
DAG_ID = "dag_with_dynamic_tasks"
TAGS = ["example", "dynamic", "task"]

LONG_DESCRIPTION = """
LONG_DESCRIPTION
"""

SHORT_DESCRIPTION = "Пример реализации DAG с динамическим списком tasks."

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=3),
}

with DAG(
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 1),
    catchup=True,
    default_args=default_args,
    tags=TAGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    start_task = EmptyOperator(task_id="start", dag=dag)

    prev_task = start_task
    
    for i in range(3):
        curr_task = PythonOperator(
            task_id="task_{}".format(i),
            python_callable=log_value,
            op_args=[i],
            dag=dag,
        )

        prev_task >> curr_task
        prev_task = curr_task

    end_task = EmptyOperator(task_id="end", dag=dag)

    prev_task >> end_task