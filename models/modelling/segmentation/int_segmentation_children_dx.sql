{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Coded complexity diagnosis evidence for the children with complexity
-- criterion. Grain: one row per person with at least one observation
-- matching the NWL CLDCHN code list (children_complexity_diagnosis_codes
-- seed), ever recorded.

SELECT
    o.person_id,
    COUNT(DISTINCT o.mapped_concept_code) AS complexity_diagnosis_codes,
    MAX(o.clinical_effective_date) AS latest_complexity_diagnosis_date
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('children_complexity_diagnosis_codes') }} AS c
    ON o.mapped_concept_code = c.snomed_code
WHERE
    o.clinical_effective_date IS NOT NULL
    AND o.clinical_effective_date <= CURRENT_DATE()
GROUP BY o.person_id
