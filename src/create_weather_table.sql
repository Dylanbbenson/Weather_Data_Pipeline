CREATE TABLE Weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    lat NUMERIC,
    lon NUMERIC,
    timezone VARCHAR(255),
    timezone_offset INT,
    current_dt INT,
    current_sunrise INT,
    current_sunset INT,
    current_temp NUMERIC,
    current_feels_like NUMERIC,
    current_pressure INT,
    current_humidity INT,
    current_dew_point NUMERIC,
    current_uvi NUMERIC,
    current_clouds INT,
    current_visibility INT,
    current_wind_speed NUMERIC,
    current_wind_deg INT,
    current_weather_0_id INT,
    current_weather_0_main VARCHAR(255),
    current_weather_0_description VARCHAR(255),
    current_weather_0_icon VARCHAR(255)

);