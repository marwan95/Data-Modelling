CREATE TABLE IF NOT EXISTS silver_layer.dim_recipient (
    recipient_key   SERIAL PRIMARY KEY,              -- Surrogate key
    profile_id      BIGINT      NOT NULL,             -- Source system ID
    npi             VARCHAR(20),
    recipient_type  VARCHAR(100),
    first_name      VARCHAR(100),
    middle_name     VARCHAR(100),
    last_name       VARCHAR(100),
    name_suffix     VARCHAR(20),
    primary_type_1  VARCHAR(100),
    primary_type_2  VARCHAR(100),
    primary_type_3  VARCHAR(100),
    specialty_1     VARCHAR(200),
    specialty_2     VARCHAR(200),
    specialty_3     VARCHAR(200),
    license_state_1 CHAR(2),
    license_state_2 CHAR(2),
    license_state_3 CHAR(2),
    -- Address (merged in per final model)
    street_line1    VARCHAR(200),
    street_line2    VARCHAR(200),
    city            VARCHAR(100),
    state           CHAR(2),
    zip_code        VARCHAR(20),
    country         VARCHAR(100),
    province        VARCHAR(100),
    postal_code     VARCHAR(20),
    -- SCD Type 2 control columns
    eff_start_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    eff_end_date    DATE,                             -- NULL = current active record
    is_current      BOOLEAN     NOT NULL DEFAULT TRUE
);

INSERT INTO silver_layer.dim_recipient (
    profile_id, npi, recipient_type,
    first_name, middle_name, last_name, name_suffix,
    primary_type_1, primary_type_2, primary_type_3,
    specialty_1, specialty_2, specialty_3,
    license_state_1, license_state_2, license_state_3,
    street_line1, street_line2, city, state, zip_code,
    country, province, postal_code,
    eff_start_date, is_current
)
SELECT DISTINCT ON (Covered_Recipient_Profile_ID)
    Covered_Recipient_Profile_ID::BIGINT,
    Covered_Recipient_NPI,
    Covered_Recipient_Type,
    Covered_Recipient_First_Name,
    Covered_Recipient_Middle_Name,
    Covered_Recipient_Last_Name,
    Covered_Recipient_Name_Suffix,
    Covered_Recipient_Primary_Type_1,
    Covered_Recipient_Primary_Type_2,
    Covered_Recipient_Primary_Type_3,
    Covered_Recipient_Specialty_1,
    Covered_Recipient_Specialty_2,
    Covered_Recipient_Specialty_3,
    Covered_Recipient_License_State_code1,
    Covered_Recipient_License_State_code2,
    Covered_Recipient_License_State_code3,
    Recipient_Primary_Business_Street_Address_Line1,
    Recipient_Primary_Business_Street_Address_Line2,
    Recipient_City,
    Recipient_State,
    Recipient_Zip_Code,
    Recipient_Country,
    Recipient_Province,
    Recipient_Postal_Code,
    CURRENT_DATE,
    TRUE
FROM bronze_layer.medical_staging
WHERE Covered_Recipient_Profile_ID IS NOT NULL
ORDER BY Covered_Recipient_Profile_ID::BIGINT;