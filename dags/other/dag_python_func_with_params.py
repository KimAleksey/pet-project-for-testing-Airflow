from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def print_info(year, month, day, hour, **context):
    print(f"year: {year}, month: {month}, day: {day}, hour: {hour}")
    print(f"{context["execution_date"]}")


DAG_OWNER = "kim-av"
DAG_NAME = "dag_python_func_with_params"
DAG_TAGS = ["example", "params"]

default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id = DAG_NAME,
    default_args = default_args,
    start_date = datetime(2025, 12,2),
    catchup = False,
    schedule_interval = "@daily",
    tags = DAG_TAGS,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    task = PythonOperator(
        task_id="printing_info",
        python_callable=print_info,
        # op_args=["{{ execution_date.year }}", "{{ execution_date.month }}", "{{ execution_date.day }}", "{{ execution_date.hour }}"],
        op_kwargs={
            "year":  "{{ execution_date.year }}",
            "month": "{{ execution_date.month }}",
            "day":   "{{ execution_date.day }}",
            "hour":  "{{ execution_date.hour }}"
        },
        dag=dag
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> task >> end