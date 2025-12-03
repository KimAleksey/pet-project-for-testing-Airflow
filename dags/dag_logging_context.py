import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DAG_OWNER = "kim-av"
DAG_NAME = "dag_logging_context"
DAG_TAGS = ["example", "context"]


def log_context(**context):
    logger = logging.getLogger(__name__)
    for key, value in context.items():
        logger.info(f"{key}: {value}")


default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_NAME,
    catchup=False,
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",
    default_args=default_args,
    tags=DAG_TAGS
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    task = PythonOperator(
        task_id='logging_context',
        python_callable=log_context,
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> task >> end_task