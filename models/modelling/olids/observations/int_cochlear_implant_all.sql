{{
    config(
       materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['adult_imms'])
}}
 --This model captures observations for a PPV vaccination clinical risk group for Cochlear Implant.
with cochlear as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

FROM ({{ get_observations("'PPV_CLINICAL_RISK_COCHLEAR_IMPLANT_COD', 'COCIMPATRISK1_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
from cochlear
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1