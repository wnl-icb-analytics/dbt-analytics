{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers Substance Misuse Interventions.

with SUBSM AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    FROM ({{ get_observations("'ILLSUBINT_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--select all to then deduplicate by person, code and date
select person_id
,clinical_effective_date
,concept_code
,concept_display
,CASE 
WHEN concept_code in ('1048091000000104','1048221000000107','417096006','112891000000107','401266006','790211000000109','4266003','417699000',
'790231000000101','201521000000104','313071005','299941000000103','866391000000106','176831000000102','1365691000000109','744857009','135828009',
'372511000000103','372541000000102') THEN 'Yes' 
WHEN concept_code in ( '299881000000104', '1091431000000107' ) THEN 'Declined'
ELSE 'Other assistance' END AS subs_misuse_services
from SUBSM 
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONCEPT_CODE, CLINICAL_EFFECTIVE_DATE ORDER BY PERSON_ID) = 1