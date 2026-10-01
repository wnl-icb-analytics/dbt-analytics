{{
    config(
        materialized='view',
        static_analysis='off'
    )
}}

/*
Number of open pathways under TFCs per patient.
Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH tfc_counts AS (
    SELECT
        sk_patient_id,
        treatment_function_code,
        COALESCE(open_pathways, 0) AS open_pathways
    FROM {{ ref('int_wl_current') }}
    WHERE sk_patient_id IS NOT NULL
)

SELECT
    *
FROM tfc_counts AS wl
PIVOT (
    SUM(open_pathways) FOR treatment_function_code IN (
        SELECT bk_specialty_code
        FROM {{ ref('stg_dictionary_dbo_specialties') }}
        WHERE is_treatment_function = TRUE
    )
) AS pvt