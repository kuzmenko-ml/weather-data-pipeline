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