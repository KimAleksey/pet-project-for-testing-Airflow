from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DAG_OWNER = "kim-av"
DAG_NAME = "dag_print_context"
DAG_TAGS = ["example", "context"]


def printing_context(**context):
    for key, value in context.items():
        print(f"{key}: {value}")


default_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2025,12, 1),
    catchup=False,
    schedule='@daily',
    default_args=default_args,
    tags=DAG_TAGS,
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    task1 = PythonOperator(
        task_id = 'print_context',
        python_callable=printing_context,
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> task1 >> end_task