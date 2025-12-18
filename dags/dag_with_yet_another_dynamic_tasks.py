from typing import Callable, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from pendulum import duration, datetime

import logging

def log_value(value: Any) -> Callable:
    def log(**context):
        logging.info(f"DAG Message: {str(value)}")
        logging.info(f"DAG ID: {context['dag'].dag_id}")
        logging.info(f"Execution Date: {context['execution_date']}")
        logging.info(f"Task Instance: {context['task_instance']}")

    return log


def generate_task_instance(**context) -> list:
    res = []
    for i in range(3):
        res.append(PythonOperator(
            task_id=f"generated_task_{i}",
            python_callable=log_value(i),
            )
        )
    return res


# Конфигурация DAG
OWNER = "kim-av"
DAG_ID = "dag_with_yet_another_dynamic_tasks"
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

    generated_tasks = generate_task_instance()

    end_task = EmptyOperator(task_id="end", dag=dag)

    (
        start_task
        >> generated_tasks
        >> end_task
    )
