from EL_pipeline import WeatherELPipeline
from TL_pipeline import WeatherTLPipeline
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def run_pipeline():
    el_pipeline = WeatherELPipeline()
    tl_pipeline = WeatherTLPipeline()

    conn = None
    try:
        print("Підключаємось до бази даних...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        print("Успішно підключено!")

        print("--- STARTING WEATHER EL PIPELINE ---")
        raw_data_from_api = el_pipeline.fetch_weather("Kyiv")

        if raw_data_from_api:
            el_pipeline.insert_raw_data(raw_data_from_api, conn)
            print("--- EL PIPELINE SUCCESSFULLY FINISHED ---")
        else:
            print("--- EL PIPELINE FAILED (No data extracted) ---")
            return
        
        print("\n--- STARTING WEATHER TL PIPELINE ---")
        raw_data = tl_pipeline.get_raw_data(conn)
        if raw_data:
            print(f"Зчитано {len(raw_data)} нових рядків для трансформації.")
        else:
            print("Немає нових даних для обробки.")
        tl_pipeline.procces_dim_city(raw_data, conn)
        tl_pipeline.procces_dim_weather_condition(raw_data,conn)
        tl_pipeline.procces_fact_weather(raw_data,conn)
        
    except Exception as e:
        print(f"\n[Критична помилка в оркестраторі]: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nПідключення до бази даних успішно закрито.")

if __name__ == "__main__": 
    run_pipeline()