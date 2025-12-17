from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from pendulum import today

import logging


def log_value(value: str, **context) -> None:
    logging.info("Info: " + value)


def group_tasks_for_logging_dates(task_group_name: str, **context) -> None:
    with TaskGroup(group_id=task_group_name, tooltip=f"Tasks for {task_group_name}") as task_group:
        log_info_date_task = PythonOperator(
            task_id="log_info_date",
            python_callable=log_value,
            op_args=[str(today())],
        )

        log_info_next_date_task = PythonOperator(
            task_id="log_info_next_date",
            python_callable=log_value,
            op_args=[str(today().add(days=1))],
        )

        log_info_prev_date_task = PythonOperator(
            task_id="log_info_prev_date",
            python_callable=log_value,
            op_args=[str(today().add(days=-1))],
        )

        log_info_date_task >> [log_info_next_date_task, log_info_prev_date_task]

        return task_group