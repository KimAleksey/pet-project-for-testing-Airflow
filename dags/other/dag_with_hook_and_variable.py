from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import logging


def reload_user_data(**context):
    postgres_hook = PostgresHook(postgres_conn_id="mydwh")
    key_name = "reload_date_for_users_count"

    try:
        reload_date = Variable.get(key_name)
    except KeyError:
        raise KeyError(f"Failed to get variable: {key_name}")

    logging.info(f"Reload date: {reload_date}")

    try:
        postgres_hook.run(
            f"""
                INSERT INTO dm.users_count (registration_date, cnt)
                SELECT 
                    login_registration_ts::date AS registration_date,
                    count(*) AS cnt
                FROM stg.users_registration
                WHERE
                    login_registration_ts::date = '{reload_date}'::date
                GROUP BY
                    login_registration_ts::date
                ON CONFLICT (registration_date) DO UPDATE SET
                    cnt = EXCLUDED.cnt
                ;
            """
        )
    except Exception as e:
        raise RuntimeError(f"Failed to reload users data: {e}")

    logging.info(f"Successfully reloaded data in table dm.users_count for date: {reload_date}")


DAG_OWNER = "kim-av"
DAG_NAME = "dag_users_hook_and_variable"
DAG_TAGS = ["example", "sensors", "hook", "variable"]


default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    tags=DAG_TAGS,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    start_task = EmptyOperator(
        task_id="start",
        dag=dag,
    )

    reloading_data_task = PythonOperator(
        task_id="reloading_data",
        python_callable=reload_user_data,
    )

    end_task = EmptyOperator(
        task_id="end",
        dag=dag,
    )