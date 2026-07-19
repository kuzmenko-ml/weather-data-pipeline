CREATE TABLE dim_city(
	city_id INT PRIMARY KEY,
	city_name VARCHAR(25),
	country VARCHAR(20),
	longitude NUMERIC,
	latitude NUMERIC,
	timezone INT
);

CREATE TABLE dim_weather_condition(
	weather_id INT PRIMARY KEY,
	main_group VARCHAR(50),
	description VARCHAR(50),
	icon VARCHAR(15)
);

CREATE TABLE fact_weather(
	record_weather_id SERIAL PRIMARY KEY,
	city_id INT REFERENCES dim_city(city_id),
	weather_id INT REFERENCES dim_weather_condition(weather_id),
	record_date TIMESTAMP,
	temperature DECIMAL(5,2),
	temp_feels_like DECIMAL(5,2),
	temp_min DECIMAL(5,2),
	temp_max DECIMAl(5,2),
	pressure INT,
	humidity INT,
	pressure_sea_level INT,
	pressure_ground_level INT,
	wind_speed DECIMAL(5,2),
	wind_direction_deg INT,
	wind_gust DECIMAL(5,2),
	sunrise TIMESTAMP,
	sunset TIMESTAMP,
	visibility INT,
	clouds_percentage INT
);

CREATE SCHEMA raw;
CREATE TABLE raw.weather_content(
	raw_id SERIAL PRIMARY KEY,
	raw_content JSONB,
	extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SCHEMA metadata;
CREATE TABLE metadata.pipeline_state(
	pipeline_name VARCHAR(25) PRIMARY KEY,
	last_processed_timestamp TIMESTAMP
);

INSERT INTO metadata.pipeline_state (pipeline_name, last_processed_timestamp) 
VALUES ('weather_transform', '2026-01-01 00:00:00');