from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from steps.flats_buildings_join_steps import create_table, extract, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='flats_buildings_join',
    description='Pipeline для объединения данных из flats и buildings в одну таблицу',
    start_date=datetime(2025, 6, 1),
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=['flats']
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        provide_context=True
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    create_table_task >> extract_task >> load_task
