{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
/*using mixed codes defined by SMI Longer Lives Campaign covers Weight Management Interventions. 
Additional codes from NHS England GPES specification. INCLUDES DIET AND EXERCISE INTERVENTIONS
*/
WITH WEIGHT_MGMT as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.cluster_id AS source_cluster_id,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display
FROM ({{ get_observations("'SMI_LONGER_LIVES_WEIGHT_MANAGEMENT','EXERCISEINT_COD','WTMGINT_COD','WTMGINTDEC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,source_cluster_id
,concept_code
,concept_display
,CASE
WHEN concept_code IN ('103699006', '306163007','443288003','11816003','306164001','424753004',
'306354000','266724001','284352003','306165000','306353006') THEN 'Yes'
WHEN concept_code in ('21701000175105','898971000000109','134385008') THEN 'Declined'
END AS referral_diet_advice
,CASE
WHEN concept_code IN ('390893007','304507003','526151000000109','183073003','416974006','767621000000102','762227003',
'913431000000101','286321000000102','429778002','439140001') THEN 'Yes'
WHEN concept_code in ('199351000000104','511991000000101','755541000000109') THEN 'Declined'
END AS referral_exercise_advice
from WEIGHT_MGMT
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1