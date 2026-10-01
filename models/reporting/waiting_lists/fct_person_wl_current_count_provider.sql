{{
    config(
        materialized='view',
        static_analysis='off'
    )
}}

/*
Number of open pathways at NCL providers per patient.
Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH provider_counts AS (
    SELECT
        sk_patient_id,
        provider_code,
        COALESCE(open_pathways, 0) AS open_pathways
    FROM {{ ref('int_wl_current') }}
    WHERE sk_patient_id IS NOT NULL
)

SELECT
    *
FROM provider_counts AS wl
PIVOT (
    SUM(open_pathways) FOR provider_code IN (
        SELECT DISTINCT reporting_code
        FROM {{ ref('stg_reference_ncl_provider') }}
        WHERE ROW_TYPE IN ('trust', 'historic_nmuh')
    )
) AS pvt