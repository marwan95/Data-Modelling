CREATE TABLE IF NOT EXISTS silver_layer.fact_payment (
    payment_key                    SERIAL PRIMARY KEY,
    recipient_key                  INT          REFERENCES silver_layer.dim_recipient(recipient_key),
    manufacturer_key               INT          REFERENCES silver_layer.dim_manufacturer(manufacturer_key),
    date_key                       INT          REFERENCES silver_layer.dim_date(date_key),
    payment_type_key               INT          REFERENCES silver_layer.dim_payment_type(payment_type_key),
    travel_key                     INT          REFERENCES silver_layer.dim_travel(travel_key),
    change_type_key                INT          REFERENCES silver_layer.lkp_change_type(change_type_key),
    dispute_status_key             INT          REFERENCES silver_layer.lkp_dispute_status(dispute_status_key),
    product_key_1                  INT          REFERENCES silver_layer.dim_product(product_key),
    product_key_2                  INT          REFERENCES silver_layer.dim_product(product_key),
    product_key_3                  INT          REFERENCES silver_layer.dim_product(product_key),
    product_key_4                  INT          REFERENCES silver_layer.dim_product(product_key),
    product_key_5                  INT          REFERENCES silver_layer.dim_product(product_key),
    -- Natural key from source
    record_id                      BIGINT       NOT NULL UNIQUE,
    -- Facts (measures)
    total_amount_usd               DECIMAL(15,2),
    num_payments_included          INT,
    -- Degenerate dimensions (low-value flags kept inline)
    charity_indicator              CHAR(3),
    physician_ownership_indicator  CHAR(3),
    delay_in_publication_indicator CHAR(3),
    related_product_indicator      CHAR(3),
    contextual_information         TEXT,
    -- Audit
    etl_loaded_at                  TIMESTAMP    NOT NULL DEFAULT NOW()
);

INSERT INTO silver_layer.fact_payment (
    recipient_key,
    manufacturer_key,
    date_key,
    payment_type_key,
    travel_key,
    change_type_key,
    dispute_status_key,
    product_key_1,
    product_key_2,
    product_key_3,
    product_key_4,
    product_key_5,
    record_id,
    total_amount_usd,
    num_payments_included,
    charity_indicator,
    physician_ownership_indicator,
    delay_in_publication_indicator,
    related_product_indicator,
    contextual_information
)
SELECT
    r.recipient_key,
    m.manufacturer_key,
    TO_CHAR(TO_DATE(src.Date_of_Payment, 'MM/DD/YYYY'), 'YYYYMMDD')::INT,
    pt.payment_type_key,
    COALESCE(tr.travel_key,
        (SELECT travel_key FROM silver_layer.dim_travel
         WHERE city_of_travel = 'N/A' LIMIT 1)),
    ct.change_type_key,
    ds.dispute_status_key,
    p1.product_key,
    p2.product_key,
    p3.product_key,
    p4.product_key,
    p5.product_key,
    src.Record_ID::BIGINT,
    src.Total_Amount_of_Payment_USDollars::DECIMAL(15,2),
    src.Number_of_Payments_Included_in_Total_Amount::INT,
    src.Charity_Indicator,
    src.Physician_Ownership_Indicator,
    src.Delay_in_Publication_Indicator,
    src.Related_Product_Indicator,
    src.Contextual_Information

FROM (
    SELECT * FROM bronze_layer.medical_staging
    LIMIT 1000000          -- 🔒 cap the source at 1M rows
) src

LEFT JOIN silver_layer.dim_recipient r
    ON  r.profile_id = src.Covered_Recipient_Profile_ID::BIGINT
    AND r.is_current = TRUE

LEFT JOIN silver_layer.dim_manufacturer m
    ON m.manufacturer_id = src.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID::BIGINT

LEFT JOIN silver_layer.dim_payment_type pt
    ON  pt.form_of_payment   = COALESCE(TRIM(src.Form_of_Payment_or_Transfer_of_Value),   'Unknown')
    AND pt.nature_of_payment = COALESCE(TRIM(src.Nature_of_Payment_or_Transfer_of_Value), 'Unknown')

LEFT JOIN silver_layer.dim_travel tr
    ON  tr.city_of_travel    = COALESCE(TRIM(src.City_of_Travel),    'N/A')
    AND tr.state_of_travel   = COALESCE(TRIM(src.State_of_Travel),   'N/A')
    AND tr.country_of_travel = COALESCE(TRIM(src.Country_of_Travel), 'N/A')

LEFT JOIN silver_layer.lkp_change_type ct
    ON ct.change_type_code = COALESCE(TRIM(src.Change_Type), 'Unknown')

LEFT JOIN silver_layer.lkp_dispute_status ds
    ON ds.dispute_status_code = COALESCE(TRIM(src.Dispute_Status_for_Publication), 'Unknown')

LEFT JOIN silver_layer.dim_product p1
    ON  p1.product_name = src.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1
    AND p1.is_current   = TRUE

LEFT JOIN silver_layer.dim_product p2
    ON  p2.product_name = src.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2
    AND p2.is_current   = TRUE

LEFT JOIN silver_layer.dim_product p3
    ON  p3.product_name = src.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3
    AND p3.is_current   = TRUE

LEFT JOIN silver_layer.dim_product p4
    ON  p4.product_name = src.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4
    AND p4.is_current   = TRUE

LEFT JOIN silver_layer.dim_product p5
    ON  p5.product_name = src.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5
    AND p5.is_current   = TRUE

ON CONFLICT (record_id) DO NOTHING;