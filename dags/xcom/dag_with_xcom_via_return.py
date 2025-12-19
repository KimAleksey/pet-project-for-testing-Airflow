from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from requests import get

DAG_OWNER = "kim-av"
DAG_NAME = "dag_with_xcom_via_return"
DAG_TAGS = ["example", "xcom", "api"]

API_URL = "https://official-joke-api.appspot.com/jokes/random"


def get_data_from_api(**context):
    response = get(API_URL).json()
    return response


def get_common_date(**context):
    common_date = context.get("data_interval_start").format("YYYYMMDDHHmmss")
    return common_date


def print_data_from_api(**context):
    ti = context['ti']
    response = ti.xcom_pull(
        key="return_value",
        task_ids="get_data",
    )
    common_date = context.get("ti").xcom_pull("get_common_date")
    print(f"Here is the joke: {response["setup"]}...{response["punchline"]}")
    print(f"Interval start: {common_date}")


default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2025, 12, 1),
    catchup=True,
    schedule_interval="0 10 * * *",
    default_args=default_args,
    tags=DAG_TAGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    get_date_task = PythonOperator(
        task_id="get_common_date",
        python_callable=get_common_date,
    )

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data_from_api,
    )

    print_data = PythonOperator(
        task_id="print_data",
        python_callable=print_data_from_api,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> get_date_task >> get_data >> print_data >> end