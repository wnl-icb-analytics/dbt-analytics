{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers Smoking Cessation Interventions.

with smok AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    FROM ({{ get_observations("'SMOKINGINT_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
,CASE 
WHEN CONCEPT_CODE in ('225323000','767641000000109','315232003','871661000000106','225324006','395700008','171055003','505281000000106','401068004') THEN 'Yes' 
WHEN concept_code in ('1087441000000106','527151000000107','871641000000105','755741000000100') THEN 'Declined'
ELSE 'Other assistance' END AS smoking_cessation_services
from smok 
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1