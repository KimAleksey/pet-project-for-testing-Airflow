from utils.working_with_pg import WorkingWithPostgres

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


from datetime import datetime, timedelta

import logging
import duckdb


def create_dm_table(**context):
    alias = WorkingWithPostgres().connect_to_postgres()

    create_dm_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {alias}.dm.users_count (
            registration_date DATE PRIMARY KEY,
            cnt INTEGER
        );
    """

    try:
        duckdb.sql(create_dm_table_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to create table {alias}.dm.users_count. {e}")

    logging.info(f"Table {alias}.dm.users_count exists.")


def get_tmp_table_name(**context):
    tmp_table_name = f"user_count_{context["task_instance"].date()}{context['task_instance'].hour}".replace("-", "")
    logging.info(f"Tmp table name: dm.user_count.")
    return tmp_table_name


def drop_tmp_table(alias, **context):
    tmp_table_name = get_tmp_table_name(**context)
    drop_tmp_table_sql = f"""
        DROP TABLE IF EXISTS {alias}.tmp.{tmp_table_name};
    """
    try:
        duckdb.sql(drop_tmp_table_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to drop table {tmp_table_name}.{e}")

    logging.info(f"Table {alias}.tmp.{tmp_table_name} was deleted.")


def create_tmp_table(**context):
    alias = WorkingWithPostgres().connect_to_postgres()
    tmp_table_name = get_tmp_table_name(**context)
    drop_tmp_table(alias, **context)
    create_tmp_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {alias}.tmp.{tmp_table_name} (
            registration_date DATE PRIMARY KEY,
            cnt INTEGER
        );
    """

    try:
        duckdb.sql(create_tmp_table_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to create tmp table {alias}.tmp.{tmp_table_name}. {e}")

    logging.info(f"Tmp table was created: tmp.{tmp_table_name}.")


def load_users_count_to_tmp_table(**context):
    alias = WorkingWithPostgres().connect_to_postgres()
    tmp_table_name = get_tmp_table_name(**context)
    load_to_tmp_table_sql = f"""
        INSERT INTO {alias}.tmp.{tmp_table_name} (registration_date, cnt)
        SELECT
            login_registration_ts::date AS registration_date,
            count(*) AS cnt
        FROM
            stg.users_registration
        WHERE
            login_registration_ts::date = '{{ ds }}'
        GROUP BY
            login_registration_ts::date;
    """

    try:
        duckdb.sql(load_to_tmp_table_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to load {alias}.tmp.{tmp_table_name}. With query: {load_to_tmp_table_sql}. {e}")

    logging.info(f"Loaded data into tmp.{tmp_table_name} for date: {{ ds }}.")


def load_data_to_users_cnt_table(**context):
    alias = WorkingWithPostgres().connect_to_postgres()
    tmp_table_name = get_tmp_table_name(**context)
    load_data_to_users_cnt_table_sql = f"""
        INSERT INTO {alias}.dm.users_count (registration_date, cnt)
        SELECT registration_date, cnt FROM {alias}.tmp.{tmp_table_name}
        ON CONFLICT (registration_date) DO UPDATE SET
            cnt = EXCLUDED.cnt
        ;
    """

    try:
        duckdb.sql(load_data_to_users_cnt_table_sql)
    except Exception as e:
        raise RuntimeError(f"Failed to load into {alias}.dm.users_count. {e}")

    logging.info(f"Loaded data into {alias}.dm.users_count.")

    drop_tmp_table(alias, **context)


DAG_OWNER = "kim-av"
DAG_NAME = "dag_load_users_from_api"
DAG_TAGS = ["example", "duckdb", "postgres", "api", "json", "sensors"]


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
    max_active_tasks=1
) as dag:

    # start_task = ExternalTaskSensor(
    #     task_id="start",
    #     external_dag_id=
    # )

    start_task = EmptyOperator(
        task_id="start",
        dag=dag,
    )

    creating_dm_table_task = PythonOperator(
        task_id="creating_dm_table",
        python_callable=create_dm_table,
    )

    creating_tmp_table_task = PythonOperator(
        task_id="creating_tmp_table",
        python_callable=create_tmp_table,
    )

    loading_users_count_to_tmp_table_task = PythonOperator(
        task_id="loading_users_count_to_tmp_table",
        python_callable=load_users_count_to_tmp_table,
    )

    loading_data_to_users_cnt_task = PythonOperator(
        task_id="loading_data_to_users_cnt",
        python_callable=load_data_to_users_cnt_table,
    )

    end_task = EmptyOperator(
        task_id="end",
        dag=dag,
    )

    start_task >> creating_dm_table_task >> creating_tmp_table_task >> loading_users_count_to_tmp_table_task >> loading_data_to_users_cnt_task >> end_task