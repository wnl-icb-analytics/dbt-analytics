{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers Medication Review.

with MED_REV AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    -- Medication Review in the last year 
    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS med_review_last_12m
FROM ({{ get_observations("'SMI_LONGER_LIVES_MEDICINES_REVIEW'", source='ECL_CACHE') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
 -- Med Rev  currency flags (standard intervals)
    ,CASE
        WHEN DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS MED_REV_LAST_12M
from MED_REV
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1