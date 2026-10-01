{{ config(materialized='table') }}

/*
Count of all active waiting lists per patient, number of waiting lists at unique providers, 
number of waiting lists under unique TFCs, and a flag for whether the patient has an open waiting list for the same TFC under multiple providers.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_data AS (
    SELECT
        sk_patient_id,
        provider_name,
        provider_code,
        treatment_function_name,
        treatment_function_code,
        days_on_waiting_list,
        days_until_future_appointment
    FROM {{ ref('int_wl_current') }}
    WHERE sk_patient_id IS NOT NULL
),

-- Identify patients with same TFC across multiple providers
-- A patient has duplicate TFCs if any TFC appears with multiple providers
duplicate_tfcs_grouped AS (
    SELECT 
        sk_patient_id,
        MAX(provider_count_per_tfc) AS tfc_multiple_providers_max_row_number
    FROM (
        SELECT 
            sk_patient_id,
            treatment_function_code,
            COUNT(DISTINCT provider_code) AS provider_count_per_tfc
        FROM base_data
        GROUP BY sk_patient_id, treatment_function_code
    )
    GROUP BY sk_patient_id
),

-- Create arrays of waiting list objects
waiting_list_arrays AS (
    SELECT
        sk_patient_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'provider', provider_name,
                'tfc_name', treatment_function_name,
                'days_on_waiting_list', days_on_waiting_list,
                'days_until_future_appointment', days_until_future_appointment
            )
        ) WITHIN GROUP (ORDER BY days_on_waiting_list) AS current_waiting_list_arrays
    FROM base_data
    GROUP BY sk_patient_id
)

SELECT
    wl.sk_patient_id,
    COUNT(*) AS wl_current_total_count,
    COUNT(DISTINCT wl.provider_code) AS wl_current_distinct_providers_count,
    COUNT(DISTINCT wl.treatment_function_code) AS wl_current_distinct_tfc_count,
    CASE 
        WHEN MAX(dtfc.tfc_multiple_providers_max_row_number) > 1 
        THEN TRUE 
        ELSE FALSE 
    END AS same_tfc_multiple_providers_flag,
    ANY_VALUE(wla.current_waiting_list_arrays) AS current_waiting_list_arrays
FROM base_data AS wl
LEFT JOIN waiting_list_arrays AS wla 
    ON wl.sk_patient_id = wla.sk_patient_id
LEFT JOIN duplicate_tfcs_grouped AS dtfc 
    ON wl.sk_patient_id = dtfc.sk_patient_id
GROUP BY wl.sk_patient_id
