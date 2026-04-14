CREATE TABLE IF NOT EXISTS silver_layer.lkp_dispute_status (
    dispute_status_key  SERIAL PRIMARY KEY,
    dispute_status_code VARCHAR(50)  NOT NULL UNIQUE,
    description         VARCHAR(200)
);

INSERT INTO silver_layer.lkp_dispute_status (dispute_status_code, description)
SELECT DISTINCT
    COALESCE(TRIM(Dispute_Status_for_Publication), 'Unknown') AS dispute_status_code,
    CASE TRIM(Dispute_Status_for_Publication)
        WHEN 'No'                THEN 'Payment is not under dispute'
        WHEN 'Yes - Resolved'    THEN 'Payment was disputed and the dispute has been resolved'
        WHEN 'Yes - Unresolved'  THEN 'Payment is currently under active dispute'
        ELSE                          'Status not specified or unrecognized'
    END AS description
FROM bronze_layer.medical_staging
WHERE Dispute_Status_for_Publication IS NOT NULL
ON CONFLICT (dispute_status_code) DO NOTHING;