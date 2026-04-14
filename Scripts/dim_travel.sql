
CREATE TABLE IF NOT EXISTS silver_layer.dim_travel (
    travel_key        SERIAL PRIMARY KEY,
    city_of_travel    VARCHAR(200),
    state_of_travel   VARCHAR(100),
    country_of_travel VARCHAR(200),
    UNIQUE (city_of_travel, state_of_travel, country_of_travel)
);
 
-- Always include an "N/A" row so the fact FK is never NULL
INSERT INTO silver_layer.dim_travel (city_of_travel, state_of_travel, country_of_travel)
VALUES ('N/A', 'N/A', 'N/A')
ON CONFLICT DO NOTHING;
 
INSERT INTO silver_layer.dim_travel (city_of_travel, state_of_travel, country_of_travel)
SELECT DISTINCT
    COALESCE(TRIM(City_of_Travel),    'N/A'),
    COALESCE(TRIM(State_of_Travel),   'N/A'),
    COALESCE(TRIM(Country_of_Travel), 'N/A')
FROM bronze_layer.medical_staging
WHERE City_of_Travel IS NOT NULL
   OR State_of_Travel IS NOT NULL
   OR Country_of_Travel IS NOT NULL
ON CONFLICT (city_of_travel, state_of_travel, country_of_travel) DO NOTHING;