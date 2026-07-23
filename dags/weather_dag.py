import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Додаємо папку src до шляхів Python, щоб Airflow бачив імпорти
sys.path.append('/opt/airflow/src')

# Імпортуємо твоє готове підключення/функцію запуску
from main import run_pipeline  # Назва файла, де лежить твоя точка входу (наприклад, main.py)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline for OpenWeather API to Postgres',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Єдина таска, яка запускає всю твою точку входу
    execute_weather_pipeline = PythonOperator(
        task_id='run_full_weather_pipeline',
        python_callable=run_pipeline,
    )