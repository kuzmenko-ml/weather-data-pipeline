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