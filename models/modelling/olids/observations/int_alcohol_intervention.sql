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
    obs.date_recorded,
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
select id AS observation_id
,date_recorded
,person_id
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
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id, concept_code, DATE(clinical_effective_date)
    ORDER BY CASE alcohol_advice_services WHEN 'Yes' THEN 1 WHEN 'Declined' THEN 2 ELSE 3 END,
        date_recorded DESC NULLS LAST, id DESC, cluster_id DESC
) = 1
