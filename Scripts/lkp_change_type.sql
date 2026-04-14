CREATE TABLE IF NOT EXISTS silver_layer.lkp_change_type (
    change_type_key   SERIAL PRIMARY KEY,
    change_type_code  VARCHAR(50)  NOT NULL UNIQUE,
    description       VARCHAR(200)
);

INSERT INTO silver_layer.lkp_change_type (change_type_code, description)
VALUES
    ('UNCHANGED', 'Record has not changed since last submission'),
    ('CHANGED',   'Record was modified from a prior submission'),
    ('NEW',       'Record appears for the first time in this program year'),
    ('DELETED',   'Record was removed from a prior submission')
ON CONFLICT (change_type_code) DO NOTHING;

INSERT INTO silver_layer.lkp_change_type (change_type_code, description)
SELECT DISTINCT
    TRIM(Change_Type),
    'UNRECOGNIZED — review source data'
FROM bronze_layer.medical_staging
WHERE Change_Type IS NOT NULL
  AND TRIM(Change_Type) NOT IN (
      SELECT change_type_code FROM silver_layer.lkp_change_type
  )
ON CONFLICT (change_type_code) DO NOTHING;
