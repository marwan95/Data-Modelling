CREATE TABLE IF NOT EXISTS silver_layer.dim_payment_type (
    payment_type_key  SERIAL PRIMARY KEY,
    form_of_payment   VARCHAR(500) NOT NULL,
    nature_of_payment VARCHAR(500) NOT NULL,
    UNIQUE (form_of_payment, nature_of_payment)
);

INSERT INTO silver_layer.dim_payment_type (form_of_payment, nature_of_payment)
SELECT DISTINCT
    COALESCE(TRIM(Form_of_Payment_or_Transfer_of_Value), 'Unknown'),
    COALESCE(TRIM(Nature_of_Payment_or_Transfer_of_Value), 'Unknown')
FROM bronze_layer.medical_staging
ON CONFLICT (form_of_payment, nature_of_payment) DO NOTHING;