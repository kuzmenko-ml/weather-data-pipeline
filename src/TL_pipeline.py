import os
from datetime import datetime
import logging

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
            logging.error(f"Помилка при роботі з базою даних: {e}")
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
                logging.info(f"Успішно додано нових міст до dim_city: {len(new_cities_to_insert)}")
            else:
                logging.warning("Нових міст для додавання не виявлено.")

        except Exception as e:
            logging.error(f"Помилка в методу procces_dim_city: {e}")
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
                    INSERT INTO public.dim_weather_condition (weather_id, main_group, description, icon)
                    VALUES (%s, %s, %s, %s);
                """
                cur.executemany(sql_insert_condition, new_conditions)

                conn.commit()
                logging.info(f"Успішно додано нових condition до dim_weather_condition: {len(new_conditions)}")
            else:
                logging.warning("Нових погодних умов для додавання не виявлено.")

        except Exception as e:
            logging.error(f"Помилка в методу procces_dim_weather_condition: {e}")
            if conn:
                conn.rollback()

        finally:
            if cur:
                cur.close()

    def procces_fact_weather(self, raw_data, conn):
        try:
            cur = conn.cursor()

            sql_query = "SELECT city_id, record_date FROM public.fact_weather"
            cur.execute(sql_query)
            existing_records_db = cur.fetchall()
            new_weather_record = []

            existing_records = []
            for row in existing_records_db:
                existing_records.append((row[0], row[1]))

            for row in raw_data:
                weather_json = row[1]

                city_id = weather_json.get("id")
                record_date_raw = weather_json.get("dt")
                record_date = datetime.fromtimestamp(record_date_raw) if record_date_raw else None

                if (city_id, record_date) in existing_records:
                    continue
                else:
                    weather_list = weather_json.get("weather", [])
                    weather_id = weather_list[0].get("id") if weather_list else None
                    temp = weather_json.get("main").get("temp")
                    temp_feels_like = weather_json.get("main").get("feels_like")
                    temp_min = weather_json.get("main").get("temp_min")
                    temp_max = weather_json.get("main").get("temp_max")
                    pressure = weather_json.get("main").get("pressure")
                    humidity = weather_json.get("main").get("humidity")
                    sea_level = weather_json.get("main").get("sea_level") 
                    ground_level = weather_json.get("main").get("ground_level")
                    wind_speed = weather_json.get("wind").get("speed")
                    wind_deg = weather_json.get("wind").get("deg")
                    wind_gust = weather_json.get("wind").get("gust")
                    sunrise_raw = weather_json.get("sys").get("sunrise")
                    sunrise = datetime.fromtimestamp(sunrise_raw) if sunrise_raw else None
                    sunset_raw = weather_json.get("sys").get("sunset")
                    sunset = datetime.fromtimestamp(sunset_raw) if sunset_raw else None
                    visibility = weather_json.get("visibility")
                    clouds = weather_json.get("clouds").get("all")

                    new_records_data = (city_id,record_date,weather_id,temp,temp_feels_like,temp_min,temp_max,pressure,humidity,
                                        sea_level,ground_level,wind_speed,wind_deg,wind_gust,sunrise,sunset,visibility,clouds)
                    
                    new_weather_record.append(new_records_data)
                    existing_records.append((city_id, record_date))

            if len(new_weather_record) > 0:
                sql_insert = """
                    INSERT INTO public.fact_weather (city_id,record_date,weather_id,temperature,temp_feels_like,temp_min,temp_max,pressure,
                                        humidity,pressure_sea_level,pressure_ground_level,wind_speed,wind_direction_deg,wind_gust,sunrise,sunset,visibility,clouds_percentage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cur.executemany(sql_insert, new_weather_record)
                conn.commit()
                logging.info(f"Успішно додано запис про погоду!")
            else:
                logging.warning("Нових погодних записів для додавання не виявлено.")
        except Exception as e:
            logging.error(f"Помилка в методу procces_fact_weather: {e}")
            if conn:
                conn.rollback()

        finally:
            if cur:
                cur.close()