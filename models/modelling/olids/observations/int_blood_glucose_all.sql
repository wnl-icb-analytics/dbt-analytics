{{
    config(
       materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry'])
}}
 --This model captures Blood Glucose measurement including fasting glucose using PCD Refset codes. Values are not categorised as they require diabetes status. Used in SMI phydical health checks
with blood_gluc as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.result_value,
    obs.result_unit_display

FROM ({{ get_observations("'FASPLASGLUC_COD','GLUC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
),

deduplicated AS (
    SELECT *
    FROM blood_gluc
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, concept_code, clinical_effective_date
        ORDER BY person_id
    ) = 1
),

fasting_codes AS (
    SELECT DISTINCT code
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE cluster_id = 'FASPLASGLUC_COD'
)

select 
glucose.person_id
,glucose.clinical_effective_date
,glucose.concept_code
,glucose.concept_display
-- Fasting membership is independent of which same-day observation survives.
,CASE WHEN fasting.code IS NOT NULL THEN 'FASPLASGLUC_COD'
    ELSE glucose.source_cluster_id END AS source_cluster_id
,glucose.result_value
,glucose.result_unit_display
from deduplicated AS glucose
LEFT JOIN fasting_codes AS fasting ON glucose.concept_code = fasting.code
