CREATE TABLE dim_city(
	city_id INT PRIMARY KEY,
	city_name VARCHAR(25),
	country VARCHAR(20),
	longitude NUMERIC,
	latitude NUMERIC,
	timezone INT
);