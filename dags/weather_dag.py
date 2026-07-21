from datetime import datetime, timedelta
import os
import logging
import psycopg2
import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.append("/opt/airflow/src")
from EL_pipeline import WeatherELPipeline
from TL_pipeline import WeatherTLPipeline
    
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

def run_el_step():
    el_pipeline = WeatherELPipeline()
    conn = get_db_connection()
    try:
        logging.info("--- ПОЧАТОК WEATHER EL PIPELINE ---")
        raw_data_from_api = el_pipeline.fetch_weather("Kyiv")

        if raw_data_from_api:
            el_pipeline.insert_raw_data(raw_data_from_api, conn)
            logging.info("--- EL PIPELINE УСПІШНО ЗАВЕРШЕНО! ---")
        else:
            raise Exception("Не вдалося отримати сирі дані з API")
    finally:
        conn.close()

def run_tl_step():
    tl_pipeline = WeatherTLPipeline()
    conn = get_db_connection()
    try:
        logging.info("--- ПОЧАТОК WEATHER TL PIPELINE ---")
        raw_data = tl_pipeline.get_raw_data(conn)
        
        if raw_data:
            logging.info(f"Зчитано {len(raw_data)} нових рядків для трансформації.")
            tl_pipeline.procces_dim_city(raw_data, conn)
            tl_pipeline.procces_dim_weather_condition(raw_data, conn)
            tl_pipeline.procces_fact_weather(raw_data, conn)
        else:
            logging.warning("Немає нових даних для обробки.")
    finally:
        conn.close()

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Автоматичний збір та обробка даних погоди',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 * * * *',  
    catchup=False
) as dag:

    task_el = PythonOperator(
        task_id='fetch_and_load_raw_weather',
        python_callable=run_el_step
    )

    task_tl = PythonOperator(
        task_id='transform_and_load_dim_fact',
        python_callable=run_tl_step
    )

    task_el >> task_tl