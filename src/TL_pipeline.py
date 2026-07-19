import os

class WeatherTLPipeline:
    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_port = os.getenv("DB_PORT")

    
    def get_raw_data(self, conn):
        try:
            cur = conn.cursor()

            sql_query_metadata = "SELECT last_processed_timestamp FROM metadata.pipeline_state WHERE pipeline_name = 'weather_transform'"
            cur.execute(sql_query_metadata)
            temp = cur.fetchone()
            last_timestamp = temp[0]

            sql_query_raw = "SELECT raw_id , raw_content , extracted_at FROM raw.weather_content WHERE extracted_at > %s"
            cur.execute(sql_query_raw,(last_timestamp,))
            raw_data = cur.fetchall()

            cur.close()

            return raw_data

        except Exception as e:
            print(f"Помилка при роботі з базою даних: {e}")
            return []
        
    def procces_dim_city(self, raw_data, conn):
        try:
            cur = conn.cursor()

            sql_query_cities = "SELECT city_id FROM public.dim_city"
            cur.execute(sql_query_cities)

            list_existing_cities = cur.fetchall()
            new_cities_to_insert = []

            existing_cities = []
            for row in list_existing_cities:
                existing_cities.append(row[0])

            for row in raw_data:
                weather_json = row[1]
                id_city = weather_json.get("id")

                if id_city in existing_cities:
                    continue
                else: 
                    city_name = weather_json.get("name")
                    country = weather_json.get("sys", {}).get("country")
                    longitude = weather_json.get("coord", {}).get("lon")
                    latitude = weather_json.get("coord", {}).get("lat")
                    timezone = weather_json.get("timezone")

                    new_city_data = (id_city, city_name, country, longitude, latitude, timezone)
                    new_cities_to_insert.append(new_city_data)

                    existing_cities.append(id_city)

            if len(new_cities_to_insert) > 0:
                sql_insert_city = """
                    INSERT INTO public.dim_city (city_id, city_name, country, longitude, latitude, timezone)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """

                cur.executemany(sql_insert_city, new_cities_to_insert)

                conn.commit()
                print(f"Успішно додано нових міст до dim_city: {len(new_cities_to_insert)}")
            else:
                print("Нових міст для додавання не виявлено.")

        except Exception as e:
            print(f"Помилка в методу procces_dim_city: {e}")
            if conn:
                conn.rollback()

        finally:
            if cur:
                cur.close()