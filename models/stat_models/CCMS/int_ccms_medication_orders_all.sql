{{ config(materialized='view') }}

-- CCMS dm+d medication evidence, one row per order and matching condition.
-- Keep the extraction as a view to preserve its existing output types.
-- Preserve future-dated orders from the existing extraction; consumers cap dates.
SELECT
    medication.person_id,
    medication.id AS medication_order_id,
    medication.clinical_effective_date::DATE AS order_date,
    medication.mapped_concept_code,
    codes.conditionid,
    codes.conditionname
FROM {{ ref('stg_olids_medication_order') }} AS medication
INNER JOIN {{ ref('stg_common_ccmc_dmd') }} AS codes
    ON medication.mapped_concept_code = codes.productid::VARCHAR
WHERE medication.clinical_effective_date IS NOT NULL
