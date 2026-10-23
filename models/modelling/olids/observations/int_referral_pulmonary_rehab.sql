{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
/*
All Pulmonary Rehab observations from clinical records.
For SMI Longer Lives Enhanced Review:
- PULRHBATT_COD: Pulmonary Rehab Attended codes
- PULRHBPU_COD: Pulmonary Rehab Unsuitable codes  
- PULRHBDEC_COD: Pulmonary Rehab Declined codes
- PULRHBOFF_COD: Pulmonary Rehab Declined codes
*/
WITH PR as (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
   
FROM ({{ get_observations("'PULRHBATT_COD', 'PULRHBPU_COD', 'PULRHBDEC_COD', 'PULRHBOFF_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,DATE(clinical_effective_date) AS clinical_effective_date
,concept_code
,concept_display
,CASE 
WHEN source_cluster_id = 'PULRHBATT_COD' THEN 'Pulmonary Rehab Attended'
WHEN source_cluster_id = 'PULRHBPU_COD' THEN 'Pulmonary Rehab Unsuitable'
WHEN source_cluster_id = 'PULRHBDEC_COD' THEN 'Pulmonary Rehab Declined'
WHEN source_cluster_id = 'PULRHBOFF_COD' THEN 'Pulmonary Rehab Offered'
END AS pr_obs_type
from PR 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY PR.person_id, PR.concept_code, DATE(PR.clinical_effective_date)
    ORDER BY PR.id
) = 1