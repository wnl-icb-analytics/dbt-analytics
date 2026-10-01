{{
    config(
       materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry'])
}}
 --This model captures observations where Smoking Status was declined by the patient.
with smok_dec as ( 
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

FROM ({{ get_observations("'SMOKSTATDEC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
from smok_dec
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1