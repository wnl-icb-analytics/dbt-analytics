{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers diet. Adding codes from NHS England GPES specification. Includes declined.

with NUTR as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.cluster_id AS source_cluster_id,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.result_value AS original_result_value,
    obs.result_unit_display
  FROM ({{ get_observations("'SMI_LONGER_LIVES_NUTRITION_ACTIVITY','NUTRIASSDEC_COD','NUTRIASS_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,source_cluster_id
,concept_code
,concept_display
,original_result_value
,result_unit_display
,CASE WHEN concept_code in ( '310502008','301991000000101','1181000119106') THEN 'Yes' 
WHEN concept_code IN ('226234005', '310503003','16208003','301961000000107','310500000') THEN 'No'
--WHEN concept_code = '401070008' AND result_unit_display = 'per day'AND original_result_value < 5 THEN 'Yes'
WHEN source_cluster_id = 'NUTRIASSDEC_COD' THEN 'Declined'
END AS poor_diet_flag
from NUTR
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1