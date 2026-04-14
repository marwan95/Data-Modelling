CREATE TABLE IF NOT EXISTS silver_layer.dim_manufacturer (
    manufacturer_key SERIAL PRIMARY KEY,
    manufacturer_id  BIGINT       NOT NULL UNIQUE,
    submitting_name  VARCHAR(200),
    paying_name      VARCHAR(200),
    state            CHAR(2),
    country          VARCHAR(100),
    is_current       BOOLEAN      NOT NULL DEFAULT TRUE
);

INSERT INTO silver_layer.dim_manufacturer (
    manufacturer_id, submitting_name, paying_name, state, country
)
SELECT DISTINCT ON (Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID)
    Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID::BIGINT,
    Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name,
    Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,
    Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State,
    Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country
FROM bronze_layer.medical_staging
WHERE Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID IS NOT NULL
ORDER BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID::BIGINT
ON CONFLICT (manufacturer_id) DO UPDATE
    SET submitting_name = EXCLUDED.submitting_name,
        paying_name     = EXCLUDED.paying_name,
        state           = EXCLUDED.state,
        country         = EXCLUDED.country;