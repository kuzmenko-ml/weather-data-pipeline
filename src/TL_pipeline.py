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

    def procces_dim_weather_condition(self, raw_data, conn):
        try:
            cur = conn.cursor()

            sql_query_condition = "SELECT weather_id FROM public.dim_weather_condition"
            cur.execute(sql_query_condition)
            list_existing_condition = cur.fetchall()
            new_conditions = []

            existing_condition = []
            for row in list_existing_condition:
                existing_condition.append(row[0])

            for row in raw_data:
                weather_json = row[1]

                weather_list = weather_json.get("weather", [])
                if not weather_list:
                    continue

                weather_data = weather_list[0] 
                condition_id = weather_data.get("id")

                if condition_id in existing_condition:
                    continue
                else:
                    main_group = weather_data.get("main")
                    description = weather_data.get("description")
                    icon = weather_data.get("icon")

                    new_data = (condition_id, main_group, description, icon)
                    new_conditions.append(new_data)

                    existing_condition.append(condition_id)

            if len(new_conditions) > 0:
                sql_insert_condition = """
                    INSERT INTO public.dim_weather_condition (condition_id, main_group, description, icon)
                    VALUES (%s, %s, %s, %s);
                """
                cur.executemany(sql_insert_condition, new_conditions)

                conn.commit()
                print(f"Успішно додано нових condition до dim_weather_condition: {len(new_conditions)}")
            else:
                print("Нових погодних умов для додавання не виявлено.")

        except Exception as e:
            print(f"Помилка в методу procces_dim_weather_condition: {e}")
            if conn:
                conn.rollback()

        finally:
            if cur:
                cur.close()