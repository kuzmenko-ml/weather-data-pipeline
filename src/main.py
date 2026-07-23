import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from EL_pipeline import WeatherELPipeline
from TL_pipeline import WeatherTLPipeline

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler()  
    ]
)

def run_pipeline():
    el_pipeline = WeatherELPipeline()
    tl_pipeline = WeatherTLPipeline()

    conn = None
    try:
        logging.info("Підключаємось до бази даних...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        logging.info("Успішно підключено!")

        logging.info("--- ПОЧАТОК WEATHER EL PIPELINE ---")
        raw_data_from_api = el_pipeline.fetch_weather("Kyiv")

        if raw_data_from_api:
            el_pipeline.insert_raw_data(raw_data_from_api, conn)
            logging.info("--- EL PIPELINE УСПІШНО ЗАВЕРШЕНО! ---")
        else:
            logging.warning("--- EL PIPELINE НЕ СПРАЦЮВАВ! (немає сирих даних) ---")
            return
        
        logging.info("--- ПОЧАТОК WEATHER TL PIPELINE ---")
        raw_data = tl_pipeline.get_raw_data(conn)
        if raw_data:
            logging.info(f"Зчитано {len(raw_data)} нових рядків для трансформації.")
            tl_pipeline.procces_dim_city(raw_data, conn)
            tl_pipeline.procces_dim_weather_condition(raw_data,conn)
            tl_pipeline.procces_fact_weather(raw_data,conn)
        else:
            logging.warning("Немає нових даних для обробки.")
        
    except Exception as e:
        logging.error(f"[Критична помилка в оркестраторі]: {e}")
        
    finally:
        if conn:
            conn.close()
            logging.info("Підключення до бази даних успішно закрито.")

if __name__ == "__main__": 
    run_pipeline()