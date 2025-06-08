CREATE TABLE public.place_master (
    place_id INT PRIMARY KEY,
    place_name VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE public.weather_readings (
    place_id INT,
    reading_date DATE,
    temperature NUMERIC(5, 2),
    rainfall NUMERIC(5, 2),
    humidity NUMERIC(5, 2)
);

CREATE TABLE public.flood_alerts (
    place_id INT,
    flood_date DATE,
    water_level NUMERIC(5, 2),
    flood_risk_level VARCHAR(20)
);

CREATE TABLE public.load_status (
    load_status_id INT PRIMARY KEY,
    status_description VARCHAR(100)
);

CREATE TABLE public.mizoram_weather_summary (
    place_id INT,
    place_name VARCHAR(100),
    month_date DATE,
    avg_temperature NUMERIC(5, 2),
    total_rainfall NUMERIC(8, 2),
    avg_humidity NUMERIC(5, 2),
    max_water_level NUMERIC(5, 2),
    flood_risk_level VARCHAR(20),
    load_status_id INT
);

########### insert_query ##########

INSERT INTO public.place_master (place_id, place_name, latitude, longitude)
SELECT 
    s.i, 
    'Place_' || s.i, 
    ROUND((23.0 + RANDOM() * (24.5-23.0))::numeric, 5),   -- Corrected cast
    ROUND((92.0 + RANDOM() * (93.5-92.0))::numeric, 5)
FROM generate_series(1, 50) AS s(i);

INSERT INTO public.weather_readings (place_id, reading_date, temperature, rainfall, humidity)
SELECT 
    FLOOR(RANDOM() * 50 + 1)::INT,
    NOW()::DATE - (FLOOR(RANDOM() * 100))::INT,
    ROUND((15 + RANDOM() * 15)::numeric, 2),
    ROUND((RANDOM() * 300)::numeric, 2),
    ROUND((60 + RANDOM() * 40)::numeric, 2)
FROM generate_series(1, 5000);

INSERT INTO public.flood_alerts (place_id, flood_date, water_level, flood_risk_level)
SELECT 
    FLOOR(RANDOM() * 50 + 1)::INT,
    NOW()::DATE - (FLOOR(RANDOM() * 100))::INT,
    ROUND((1 + RANDOM() * 5)::numeric, 2),
    (ARRAY['Low', 'Medium', 'High'])[FLOOR(RANDOM() * 3 + 1)]
FROM generate_series(1, 2000);

INSERT INTO public.load_status (load_status_id, status_description)
VALUES
(1, 'Success'),
(2, 'Failed'),
(3, 'Partial');
