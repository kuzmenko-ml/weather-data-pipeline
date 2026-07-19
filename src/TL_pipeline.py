import os
import psycopg2

class WeatherTLPipeline:
    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_port = os.getenv("DB_PORT")

    
    def get_raw_data(self):
        try:
            print("Підключаємось до бази даних...")
            conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port
            )

            cur = conn.cursor()
            print("Успішно підключено до бази даних!")

            sql_query_metadata = "SELECT last_processed_timestamp FROM metadata.pipeline_state WHERE pipeline_name = 'weather_transform'"
            cur.execute(sql_query_metadata)
            temp = cur.fetchone()
            last_timestamp = temp[0]

            sql_query_raw = "SELECT raw_id , raw_content , extracted_at FROM raw.weather_content WHERE extracted_at > %s"
            cur.execute(sql_query_raw,(last_timestamp,))
            raw_data = cur.fetchall()

            cur.close()
            conn.close()

            return raw_data

        except Exception as e:
            print(f"Помилка при роботі з базою даних: {e}")