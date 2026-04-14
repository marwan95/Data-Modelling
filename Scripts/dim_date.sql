CREATE TABLE IF NOT EXISTS silver_layer.dim_date (
    date_key           INT PRIMARY KEY,   -- Format: YYYYMMDD
    full_date          DATE        NOT NULL,
    day_of_week        VARCHAR(10) NOT NULL,
    day_number         INT         NOT NULL,
    week_number        INT         NOT NULL,
    month_number       INT         NOT NULL,
    month_name         VARCHAR(15) NOT NULL,
    quarter            INT         NOT NULL,
    year               INT         NOT NULL,
    is_weekend         BOOLEAN     NOT NULL,
    is_federal_holiday BOOLEAN     NOT NULL DEFAULT FALSE,
    program_year       INT
);

INSERT INTO silver_layer.dim_date (
    date_key, full_date, day_of_week, day_number,
    week_number, month_number, month_name, quarter,
    year, is_weekend, program_year
)
SELECT DISTINCT
    TO_CHAR(d, 'YYYYMMDD')::INT              AS date_key,
    d                                         AS full_date,
    TO_CHAR(d, 'Day')                         AS day_of_week,
    EXTRACT(DAY FROM d)::INT                  AS day_number,
    EXTRACT(WEEK FROM d)::INT                 AS week_number,
    EXTRACT(MONTH FROM d)::INT                AS month_number,
    TO_CHAR(d, 'Month')                       AS month_name,
    EXTRACT(QUARTER FROM d)::INT              AS quarter,
    EXTRACT(YEAR FROM d)::INT                 AS year,
    EXTRACT(ISODOW FROM d) IN (6, 7)          AS is_weekend,
    Program_Year::INT                         AS program_year
FROM (
    SELECT
        TO_DATE(Date_of_Payment, 'MM/DD/YYYY') AS d,
        Program_Year::INT                       AS Program_Year
    FROM bronze_layer.medical_staging
    WHERE Date_of_Payment IS NOT NULL
) dated
ON CONFLICT (date_key) DO NOTHING;