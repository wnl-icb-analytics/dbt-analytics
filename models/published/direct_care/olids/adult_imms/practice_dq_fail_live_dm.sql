{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}
--practice list with failing EMIS registration data quality checks. medicus select care not reported in EMIS
select  eq.borough 
,eq.practice_code 
,eq.practice_name 
,eq.issue_category     
from {{ ref('emis_olids_reg_fail_direct_care') }} eq   
LEFT join (select distinct BOROUGH_REGISTERED, practice_code, practice_name 
from {{ ref('dim_person_demographics') }}) p using (practice_code)   
where practice_code <> 'Y03103' order by 1, 2 