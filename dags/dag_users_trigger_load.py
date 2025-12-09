from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta

DAG_OWNER = "kim-av"
DAG_NAME = "dag_users_trigger_load"
DAG_TAGS = ["example", "sensors", "trigger"]


default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    catchup=True,
    schedule_interval=timedelta(hours=1),
    tags=DAG_TAGS,
    max_active_runs=3,
    max_active_tasks=3
) as dag:

    start_task = ExternalTaskSensor(
        task_id="start",
        external_dag_id="dag_load_users_from_api",
        external_task_id="end_task",
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    trigger_dag_task = TriggerDagRunOperator(
        trigger_dag_id="dag_load_users_dm_started_by_trigger",
        task_id="trigger_dag",
        wait_for_completion=False,
    )

    end_task = EmptyOperator(
        task_id="end",
        dag=dag,
    )

    start_task >> trigger_dag_task >> end_task