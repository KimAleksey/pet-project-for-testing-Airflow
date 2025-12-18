import json
import logging

from typing import Any, Callable
from pathlib import Path
from pendulum import datetime, duration

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def get_configs(file_name: str = "dag_config.json") -> list[dict[str, Any]]:
    """
     Загружает конфиги для генерации DAGs

    :param file_name: Путь к JSON файлу с конфигурацией
    :return: Список конфигураций
    """
    current_path = Path(__file__).parent.parent
    file_path = current_path / "conf" / file_name

    with open(file_path, "r", encoding="utf-8") as f:
        res = json.load(f)

    return res["dags"]


def log_info(message: str) -> Callable:
    def log(**context):
        logging.info(message)

    return log


def generate_dags(config: dict[str, Any]) -> DAG:
    """
    Генерируем DAG на основании конфигурационного файла.

    :param config: Конфиг для DAG
    :return: DAG
    """
    start_date = datetime(2025, 12, 1)

    default_args = {
        "owner": config["owner"],
        "depends_on_past": config["depends_on_past"],
        "retries": config["retries"],
        "retry_delay": duration(minutes=3),
    }

    with DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        schedule_interval=config["schedule_interval"],
        start_date=start_date,
        catchup=config["catchup"],
        max_active_runs=config["max_active_runs"],
        max_active_tasks=config["max_active_tasks"],
        tags=config["tags"],
    ) as dag:

        start_task = EmptyOperator(task_id="start", dag=dag)

        logging_task = PythonOperator(
            task_id="logging",
            python_callable=log_info(config["value"]),
            dag=dag,
        )

        end_task = EmptyOperator(task_id="end", dag=dag)

        start_task >> logging_task >> end_task

    return dag


try:
    configs = get_configs()

    for config in configs:
        dag_id = config["dag_id"]
        globals()[dag_id] = generate_dags(config)

except Exception as e:
    logging.error(f"Failed to create DAGs: {e}.")