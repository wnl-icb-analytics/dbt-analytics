{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}

/*
MH/SMI comprehensive care plan QOF Indicator. Date of the mental health care plan code MHP_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/
with planmh as (
SELECT
    obs.id,
    obs.date_recorded,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

    -- All records represent completed MH Care Plan
    TRUE AS is_completed_MH_care_plan,

    -- MH Care Plan  currency flags (standard intervals)
    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS MH_care_plan_current_12m

FROM ({{ get_observations("'MHP_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select id AS observation_id
,date_recorded
,person_id
,clinical_effective_date
,concept_code
,concept_display
,MH_care_plan_current_12m
from planmh
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY date_recorded DESC NULLS LAST, id DESC) = 1