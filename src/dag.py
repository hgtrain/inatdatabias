from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from kafka_producer import get_year, listen
from datetime import datetime

with DAG(
    dag_id="historical_data",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@once"
) as historical:
    task_2025 = PythonOperator(
        task_id="2025_data",
        python_callable=get_year(2025)
    )
    task_2020 = PythonOperator(
        task_id="2020_data",
        python_callable=get_year(2020)
    )
    task_2019 = PythonOperator(
        task_id="2019_data",
        python_callable=get_year(2019)
    )
    [task_2025, task_2020, task_2019]
    # >> consumer >> loader

with DAG(
    dag_id="daily_data",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily"
) as daily:
    get_today = PythonOperator(
        task_id="today_data"
        # python_callable=
    )
