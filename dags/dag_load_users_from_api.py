from utils.working_with_pg import WorkingWithPostgres

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from requests import get
from datetime import datetime, timedelta

import duckdb
import logging

def create_target_table(**context):

    alias = WorkingWithPostgres().connect_to_postgres()

    try:
        duckdb.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {alias}.stg.users_registration (
                login_uuid uuid PRIMARY KEY,
                login_registration_ts TIMESTAMP,
                login_username TEXT,
                login_password TEXT,
                name_title TEXT,
                name_first TEXT,
                name_last TEXT,
                location_street_number INT,
                location_street_name TEXT,
                location_city TEXT,
                location_state TEXT,
                location_country TEXT,
                location_postcode TEXT,
                location_coordinates_latitude NUMERIC,
                location_coordinates_longitude NUMERIC,
                email TEXT,
                age INT,
                phone TEXT,
                cell TEXT
            );
            """
        )
    except Exception as error:
        raise RuntimeError(f"Failed to create target table. {error}")

    logging.info(f"Target table was successfully created: {alias}.stg.users_registration")


def get_tmp_table_name(**context) -> str:
    tmp_table_name = f"users_registration_{context["execution_date"].date()}{context["execution_date"].hour}".replace("-", "")
    return tmp_table_name


def delete_tmp_table(alias, **context):

    tmp_table_name = get_tmp_table_name(**context)

    delete_tmp_table_tmp_table_sql = f"""
        DROP TABLE IF EXISTS {alias}.tmp.{tmp_table_name} CASCADE;
        """

    try:
        duckdb.sql(delete_tmp_table_tmp_table_sql)
    except Exception as error:
        raise RuntimeError(f"Failed to delete tmp table. {error}")

    logging.info(f"tmp table was successfully deleted: {alias}.tmp.{tmp_table_name}")


def create_tmp_table(**context):

    alias = WorkingWithPostgres().connect_to_postgres()
    tmp_table_name = get_tmp_table_name(**context)

    delete_tmp_table(alias, **context)

    create_tmp_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {alias}.tmp.{tmp_table_name} (
            login_uuid uuid PRIMARY KEY,
            login_registration_ts TIMESTAMP,
            login_username TEXT,
            login_password TEXT,
            name_title TEXT,
            name_first TEXT,
            name_last TEXT,
            location_street_number INT,
            location_street_name TEXT,
            location_city TEXT,
            location_state TEXT,
            location_country TEXT,
            location_postcode TEXT,
            location_coordinates_latitude NUMERIC,
            location_coordinates_longitude NUMERIC,
            email TEXT,
            age INT,
            phone TEXT,
            cell TEXT
        );
    """

    try:
        duckdb.sql(create_tmp_table_sql)
    except Exception as error:
        raise RuntimeError(f"Failed to create target tmp table: {alias}.tmp.{tmp_table_name}. {error}")

    logging.info(f"Target table was successfully created: {alias}.tmp.{tmp_table_name}")


def get_data_from_api(**context):

    ti = context["ti"]
    api_url = "https://randomuser.me/api/?format=json"

    try:
        response = get(api_url, timeout=5)
    except Exception as error:
        raise RuntimeError(f"Failed to get data from API. {error}")

    if response.status_code != 200:
        raise RuntimeError(f"API returned unexpected status code: {response.status_code}.")

    api_data = response.json()["results"][0]
    try:
        ti.xcom_push(key="user_registration_data", value=api_data)
    except Exception as error:
        raise RuntimeError(f"Failed to push data to XCOM. {error}")

    logging.info(f"Data retrieved from API: {api_data}")


def load_data_to_tmp_table(**context):
    ti = context["ti"]

    try:
        user_data = ti.xcom_pull(key="user_registration_data", task_ids="getting_data_from_api")
    except Exception as error:
        raise RuntimeError(f"Failed to get data from XCOM. {error}")

    alias = WorkingWithPostgres().connect_to_postgres()

    column_list = f"""
        login_uuid, login_registration_ts, login_username,
        login_password, name_title, name_first,
        name_last, location_street_number, location_street_name,
        location_city, location_state, location_country,
        location_postcode, location_coordinates_latitude, location_coordinates_longitude,
        email, age, phone, cell
    """

    values_str = f"""
        '{user_data['login']['uuid']}',
         '{context["execution_date"]}',
         '{user_data['login']['username']}',
         '{user_data['login']['password']}',
         '{user_data['name']['title']}',
         '{user_data['name']['first']}',
         '{user_data['name']['last']}',
          {user_data['location']['street']['number']},
         '{user_data['location']['street']['name']}',
         '{user_data['location']['city']}',
         '{user_data['location']['state']}',
         '{user_data['location']['country']}',
         '{user_data['location']['postcode']}',
         {user_data['location']['coordinates']['latitude']},
         {user_data['location']['coordinates']['longitude']},
         '{user_data['email']}',
         {user_data['registered']['age']},
         '{user_data['phone']}',
         '{user_data['cell']}'
    """

    tmp_table_name = get_tmp_table_name(**context)

    insert_values_sql = f"""
        INSERT INTO {alias}.tmp.{tmp_table_name} ({column_list})
        VALUES ({values_str})
        ;
    """

    print(insert_values_sql)

    try:
        duckdb.sql(insert_values_sql)
    except Exception as error:
        raise RuntimeError(f"Failed to insert data into table {alias}.tmp.{tmp_table_name}. {error}")


def load_data_to_target_table(**context):

    alias = WorkingWithPostgres().connect_to_postgres()

    column_list = f"""
        login_uuid, login_registration_ts, login_username,
        login_password, name_title, name_first,
        name_last, location_street_number, location_street_name,
        location_city, location_state, location_country,
        location_postcode, location_coordinates_latitude, location_coordinates_longitude,
        email, age, phone, cell
    """
    tmp_table_name = get_tmp_table_name(**context)

    insert_sql = f"""
        INSERT INTO {alias}.stg.users_registration ({column_list})
        SELECT {column_list} FROM {alias}.tmp.{tmp_table_name}
        ON CONFLICT (login_uuid) DO UPDATE SET
            login_registration_ts           = EXCLUDED.login_registration_ts,
            login_username                  = EXCLUDED.login_username,
            login_password                  = EXCLUDED.login_password, 
            name_title                      = EXCLUDED.name_title, 
            name_first                      = EXCLUDED.name_first,
            name_last                       = EXCLUDED.name_last, 
            location_street_number          = EXCLUDED.location_street_number, 
            location_street_name            = EXCLUDED.location_street_name,
            location_city                   = EXCLUDED.location_city, 
            location_state                  = EXCLUDED.location_state, 
            location_country                = EXCLUDED.location_country,
            location_postcode               = EXCLUDED.location_postcode, 
            location_coordinates_latitude   = EXCLUDED.location_coordinates_latitude, 
            location_coordinates_longitude  = EXCLUDED.location_coordinates_longitude,
            email                           = EXCLUDED.email, 
            age                             = EXCLUDED.age, 
            phone                           = EXCLUDED.phone, 
            cell                            = EXCLUDED.cell
        ;
    """

    try:
        duckdb.sql(insert_sql)
    except Exception as error:
        raise RuntimeError(f"Failed to insert data into table stg.users_registration. {error}")

    logging.info(f"Data successfully loaded into table {alias}.stg.users_registration.")

    delete_tmp_table(alias, **context)


DAG_OWNER = "kim-av"
DAG_NAME = "dag_load_users_from_api"
DAG_TAGS = ["example", "duckdb", "postgres", "api", "json"]


default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with (DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=True,
    start_date=datetime(2025, 12, 1),
    schedule_interval=timedelta(hours=1),
    tags=DAG_TAGS,
    max_active_tasks=2,
    max_active_runs=1,
) as dag):

    start_task = EmptyOperator(task_id="start_task", dag=dag)

    creating_target_table_task = PythonOperator(
        task_id="creating_target_table",
        python_callable=create_target_table,
    )

    creating_tmp_table_task = PythonOperator(
        task_id="creating_tmp_table",
        python_callable=create_tmp_table,
    )

    getting_data_from_api_task = PythonOperator(
        task_id="getting_data_from_api",
        python_callable=get_data_from_api,
    )

    loading_data_to_tmp_table_task = PythonOperator(
        task_id="loading_data_to_tmp_table",
        python_callable=load_data_to_tmp_table,
    )

    loading_data_to_target_table_task = PythonOperator(
        task_id="loading_data_to_target_table",
        python_callable=load_data_to_target_table,
    )

    end_task = EmptyOperator(task_id="end_task", dag=dag)

    start_task >> [creating_target_table_task, creating_tmp_table_task] >> getting_data_from_api_task
    getting_data_from_api_task >> loading_data_to_tmp_table_task >> loading_data_to_target_table_task >> end_task