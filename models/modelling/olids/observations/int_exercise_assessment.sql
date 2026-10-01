{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers exercise habits. Additional codes from NHS England GPES specification.

with EXER as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.cluster_id AS source_cluster_id,
    obs.mapped_concept_display AS concept_display
  FROM ({{ get_observations("'SMI_LONGER_LIVES_EXERCISE_ASSESSMENT', 'EXERASS_COD','EXERASSDEC_COD'") }}) obs
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
WHEN concept_code IN ('228445002','160631001','365981000000101','413462007','228448000','413460004','160641003','963051000000100',
'86047003','366211000000105','160638007','225924002','160644006','160643000','413461000','160642005','102533007',
'366241000000106','160639004','225925001','160645007') THEN 'Yes' 
WHEN concept_code IN ('160632008','160633003','366121000000108','160630000','160636006','160640002','366171000000107',
'160637002','228446001','40979000','366061000000101','160652009','963071000000109') THEN 'No'
WHEN concept_code = '160629005' THEN 'Exercise physically impossible'
WHEN source_cluster_id = 'EXERASSDEC_COD' THEN 'Declined'
ELSE 'Unknown'
END AS low_exercise_flag
from EXER
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1