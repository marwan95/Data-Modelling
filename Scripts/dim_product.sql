CREATE TABLE IF NOT EXISTS silver_layer.dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_name        VARCHAR(500),
    ndc_code            VARCHAR(50),
    pdi_code            VARCHAR(50),
    therapeutic_area    VARCHAR(500),
    drug_or_device_flag VARCHAR(200),
    covered_indicator   VARCHAR(100),
    eff_start_date      DATE         NOT NULL DEFAULT CURRENT_DATE,
    eff_end_date        DATE,
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE
);

INSERT INTO silver_layer.dim_product (
    product_name, ndc_code, pdi_code,
    therapeutic_area, drug_or_device_flag, covered_indicator
)
SELECT DISTINCT
    product_name,
    ndc_code,
    pdi_code,
    therapeutic_area,
    drug_or_device_flag,
    covered_indicator
FROM (
    SELECT
        Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 AS product_name,
        Associated_Drug_or_Biological_NDC_1                       AS ndc_code,
        Associated_Device_or_Medical_Supply_PDI_1                 AS pdi_code,
        Product_Category_or_Therapeutic_Area_1                    AS therapeutic_area,
        Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1 AS drug_or_device_flag,
        Covered_or_Noncovered_Indicator_1                         AS covered_indicator
    FROM bronze_layer.medical_staging WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 IS NOT NULL
 
    UNION
 
    SELECT
        Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2,
        Associated_Drug_or_Biological_NDC_2,
        Associated_Device_or_Medical_Supply_PDI_2,
        Product_Category_or_Therapeutic_Area_2,
        Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2,
        Covered_or_Noncovered_Indicator_2
    FROM bronze_layer.medical_staging WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 IS NOT NULL
 
    UNION
 
    SELECT
        Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3,
        Associated_Drug_or_Biological_NDC_3,
        Associated_Device_or_Medical_Supply_PDI_3,
        Product_Category_or_Therapeutic_Area_3,
        Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3,
        Covered_or_Noncovered_Indicator_3
    FROM bronze_layer.medical_staging WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 IS NOT NULL
 
    UNION
 
    SELECT
        Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4,
        Associated_Drug_or_Biological_NDC_4,
        Associated_Device_or_Medical_Supply_PDI_4,
        Product_Category_or_Therapeutic_Area_4,
        Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4,
        Covered_or_Noncovered_Indicator_4
    FROM bronze_layer.medical_staging WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 IS NOT NULL
 
    UNION
 
    SELECT
        Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5,
        Associated_Drug_or_Biological_NDC_5,
        Associated_Device_or_Medical_Supply_PDI_5,
        Product_Category_or_Therapeutic_Area_5,
        Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5,
        Covered_or_Noncovered_Indicator_5
    FROM bronze_layer.medical_staging WHERE Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 IS NOT NULL
) all_products;