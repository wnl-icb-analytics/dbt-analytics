{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign Alcohol Misuse education and referral to treatment. Added 2 extra codes from PCDrefset. Also include declined treatment codes.
WITH ALC as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id
FROM ({{ get_observations("'SMI_LONGER_LIVES_ALCOHOL_EDUCATION','ALCOHOLINT_COD','ALCSPADVDEC_COD','ALCBRINTDEC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
,CASE 
WHEN cluster_id = 'ALCOHOLINT_COD' THEN 'Yes'
WHEN cluster_id in ('ALCSPADVDEC_COD','ALCBRINTDEC_COD') THEN 'Declined' 
WHEN cluster_id = 'SMI_LONGER_LIVES_ALCOHOL_EDUCATION' AND concept_code <> '21121000175100' THEN 'Yes' 
WHEN cluster_id = 'SMI_LONGER_LIVES_ALCOHOL_EDUCATION' AND concept_code = '21121000175100' THEN 'Declined'
END AS alcohol_advice_services
from ALC
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1
