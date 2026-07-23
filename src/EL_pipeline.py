import os
import requests
from dotenv import load_dotenv
import psycopg2
import json
import logging

load_dotenv()

class WeatherELPipeline:
    def __init__(self):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")

    def fetch_weather(self, city_name="Kyiv"):
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={self.api_key}&units=metric"

        try:
            response = requests.get(url)
            response.raise_for_status()
            logging.info("Успішний запит до API.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Помилка при запиті до API: {e}")
            return None
        
    def insert_raw_data(self, data, conn):
        if data is None:
            logging.warning("Немає даних для запису в базу.")
            return
        
        cur = None
        try:
            cur = conn.cursor()

            json_data_string = json.dumps(data)
            sql_query = "INSERT INTO raw.weather_content (raw_content) VALUES (%s);"
            cur.execute(sql_query, (json_data_string,))
            conn.commit()
            logging.info("Дані успішно записані в таблицю raw.weather_content!")

            cur.close()
            
        except Exception as e:
            logging.error(f"Помилка при роботі з базою даних: {e}")
            if conn:
                conn.rollback()
        
        finally:
            if cur:
                cur.close()