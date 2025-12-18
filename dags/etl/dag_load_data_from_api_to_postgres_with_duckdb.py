import duckdb
import requests

from datetime import timedelta, datetime

from utils.working_with_pg import WorkingWithPostgres

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import logging

def create_table_with_duckdb(**context):

    credentials = WorkingWithPostgres.get_db_credentials()

    alias = WorkingWithPostgres.connect_to_postgres_via_duckdb(
        dbname=credentials["POSTGRES_DB"],
        host=credentials["POSTGRES_HOST"],
        port=credentials["POSTGRES_PORT"],
        user=credentials["POSTGRES_USER_NAME"],
        password=credentials["POSTGRES_PASSWORD"],
        alias="db"
    )

    if not alias:
        raise RuntimeError("Failed to connect to PostgreSQL")

    try:
        duckdb.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {alias}.public.jokes_every_day (
                id INT PRIMARY KEY,
                load_ts timestamp,
                type varchar,
                setup varchar,
                punchline varchar
            );
            """
        )
    except Exception as e:
        raise RuntimeError(f"Failed to create table: {e}")


def get_data_from_api(**context):

    api_url = "https://official-joke-api.appspot.com/jokes/random"

    try:
        request = requests.get(api_url)
    except Exception as e:
        raise RuntimeError(f"Failed to get data from API: {e}. Using URL: {api_url}")
    joke_data = request.json()
    ti = context["ti"]
    ti.xcom_push(key="joke_data_json", value=joke_data)
    logging.info(f"Jokes data was successfully retrieved: {joke_data}.")


def load_data_to_postgres(**context):
    ti = context["ti"]
    joke_data = ti.xcom_pull(key="joke_data_json", task_ids="get_data")

    credentials = WorkingWithPostgres.get_db_credentials()

    alias = WorkingWithPostgres.connect_to_postgres_via_duckdb(
        dbname=credentials["POSTGRES_DB"],
        host=credentials["POSTGRES_HOST"],
        port=credentials["POSTGRES_PORT"],
        user=credentials["POSTGRES_USER_NAME"],
        password=credentials["POSTGRES_PASSWORD"],
        alias="db"
    )

    insert_sql = f"""
        INSERT INTO {alias}.public.jokes_every_day (id, load_ts, type, setup, punchline)
        VALUES ({int(joke_data["id"])}, 
                '{datetime.now()}', 
                '{joke_data["type"].replace("'", "''")}', 
                '{joke_data["setup"].replace("'", "''")}', 
                '{joke_data["punchline"].replace("'", "''")}'
            )
        ON CONFLICT (id) DO UPDATE SET 
            load_ts = EXCLUDED.load_ts,
            type = EXCLUDED.type,
            setup = EXCLUDED.setup,
            punchline = EXCLUDED.punchline
        ;
    """

    try:
        duckdb.sql(insert_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to insert into table {alias}.public.jokes_every_day: {e}. Using insert statement: {insert_sql}.")

    logging.info(f"Jokes data was successfully inserted: {joke_data}.")

DAG_OWNER = "kim-av"
DAG_NAME = "dag_load_data_to_pg_with_duckdb"
DAG_TAGS = ["example", "duckdb", "postgres", "api"]


default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=True,
    start_date=datetime(2025, 12, 1),
    schedule_interval="0 * * * *",
    tags=DAG_TAGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    creating_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_with_duckdb,
    )

    getting_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data_from_api,
    )

    loading_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> creating_table >> getting_data >> loading_data >> end