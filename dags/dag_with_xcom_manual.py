from requests import get

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

API_URL = "https://official-joke-api.appspot.com/jokes/random"


def get_data_from_api(**context):
    ti = context['ti']
    response = get(API_URL).json()
    ti.xcom_push(key="api_data", value=response)


def print_data(**context):
    ti = context['ti']
    response = ti.xcom_pull(
        task_ids = "getting_data",
        key = "api_data",
    )
    print(f"Here is the joke: {response["setup"]}...{response["punchline"]}")


DAG_OWNER = "kim-av"
DAG_NAME = "dag_with_xcom_manual"
DAG_TAGS = ["example", "xcom", "api"]


default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    catchup=True,
    schedule_interval=timedelta(days=1),
    tags=DAG_TAGS,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    getting_data = PythonOperator(
        task_id="getting_data",
        python_callable=get_data_from_api,
    )

    printing_data = PythonOperator(
        task_id="printing_data",
        python_callable=print_data,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> getting_data >> printing_data >> end