import os
import requests
from dotenv import load_dotenv

load_dotenv()

class WeatherELPipeline:
    def __init__(self):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_port = os.getenv("DB_PORT")

    def fetch_weather(self, city_name="Kyiv"):
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={self.api_key}&units=metric"

        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Помилка при запиті до API: {e}")
            return None