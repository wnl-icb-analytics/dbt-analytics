{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using all 3 prompts defined by SMI Enhanced health checks for Cancer Screening campaign and filter to Cervical Screening prompts only
WITH ALL_SCREENING_PROMPTS as (
SELECT
    obs.id,
    obs.date_recorded,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
FROM ({{ get_observations("'SMI_LONGER_LIVES_CANCER_SCREEN_PROMPTS'", source='ECL_CACHE') }}) obs
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
from ALL_SCREENING_PROMPTS
WHERE concept_code = '851141000000101' -- SNOMED code for Cervical Screening Prompt
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, clinical_effective_date ORDER BY date_recorded DESC NULLS LAST, id DESC) = 1