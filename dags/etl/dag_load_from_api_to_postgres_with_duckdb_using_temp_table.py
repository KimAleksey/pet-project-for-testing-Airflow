import duckdb
import requests

from datetime import timedelta, datetime

from utils.working_with_pg import WorkingWithPostgres

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import logging

def create_table_with_duckdb(**context):

    alias = WorkingWithPostgres.connect_to_postgres()

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


def create_tmp_table(**context):

    alias = WorkingWithPostgres.connect_to_postgres()

    try:
        duckdb.sql(
            f"""
            DROP TABLE IF EXISTS {alias}.tmp.jokes_every_day_tmp;
            
            CREATE TABLE IF NOT EXISTS {alias}.tmp.jokes_every_day_tmp (
                id INT,
                load_ts timestamp,
                type varchar,
                setup varchar,
                punchline varchar
            );
            """)
    except Exception as e:
        raise RuntimeError(f"Failed to create temp table: {e}")

    logging.info(f"Temp table was successfully created: {alias}.tmp.jokes_every_day_tmp")


def load_to_tmp_table(**context):

    api_url = "https://official-joke-api.appspot.com/jokes/random"

    alias = WorkingWithPostgres.connect_to_postgres()

    try:
        request = requests.get(api_url)
    except Exception as e:
        raise RuntimeError(f"Failed to get data from API: {e}. Using URL: {api_url}")

    joke_data = request.json()
    logging.info(f"Jokes data was successfully retrieved: {joke_data}.")

    insert_sql = f"""
        INSERT INTO {alias}.tmp.jokes_every_day_tmp (id, load_ts, type, setup, punchline)
        VALUES ({int(joke_data["id"])}, 
                '{datetime.now()}', 
                '{joke_data["type"].replace("'", "''")}', 
                '{joke_data["setup"].replace("'", "''")}', 
                '{joke_data["punchline"].replace("'", "''")}'
            )
        ;
    """

    try:
        duckdb.sql(insert_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to load data from API into temp table: {e}.")


def load_data_to_postgres(**context):

    alias = WorkingWithPostgres.connect_to_postgres()

    try:
        duckdb.sql(
            f"""
            DELETE FROM {alias}.public.jokes_every_day 
            WHERE 
                id in (SELECT id FROM {alias}.tmp.jokes_every_day_tmp)
            ;

            INSERT INTO {alias}.public.jokes_every_day (id, load_ts, type, setup, punchline)
            SELECT id, load_ts, type, setup, punchline FROM {alias}.tmp.jokes_every_day_tmp
            ;
            """
        )
    except Exception as e:
        raise RuntimeError(f"Failed to insert into table {alias}.public.jokes_every_day: {e}.")

    logging.info(f"Jokes data was successfully inserted.")
    logging.info(f"Table {alias}.tmp.jokes_every_day_tmp was deleted.")


DAG_OWNER = "kim-av"
DAG_NAME = "dag_load_data_to_pg_with_duckdb_using_temp_table"
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

    creating_tmp_table = PythonOperator(
        task_id="create_tmp_table",
        python_callable=create_tmp_table,
    )

    loading_data_to_tmp_table = PythonOperator(
        task_id="load_data_to_tmp_table",
        python_callable=load_to_tmp_table,
    )

    loading_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> creating_table >> creating_tmp_table >> loading_data_to_tmp_table >> loading_data >> end