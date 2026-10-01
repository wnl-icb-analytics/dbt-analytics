{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}

{% set placeholder_date = '1900-01-01'%} 

/*
Patient data processing for CLTCS

Clinical Purpose:
- Details LTCS summary data for patients with complex needs in C-LTCS

*/

with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
)

select il.patient_id
    , il.area_code
    , ltcs.condition_code
    , ltcs.condition_name
    , ltcs.clinical_domain
    , ltcs.is_on_register
    , ltcs.is_qof
    , CASE 
        WHEN ltcs.earliest_diagnosis_date IS NULL THEN '{{placeholder_date}}'
        WHEN ltcs.earliest_diagnosis_date > CURRENT_DATE() THEN '{{placeholder_date}}'  -- Future dates invalid
        WHEN ltcs.earliest_diagnosis_date < DATEADD('year', -150, CURRENT_DATE()) THEN '{{placeholder_date}}'  -- Unreasonably old dates
        ELSE DATE(ltcs.earliest_diagnosis_date)
      END as earliest_diagnosis_date
    , CASE 
        WHEN ltcs.latest_diagnosis_date IS NULL THEN '{{placeholder_date}}'
        WHEN ltcs.latest_diagnosis_date > CURRENT_DATE() THEN '{{placeholder_date}}'  -- Future dates invalid
        WHEN ltcs.latest_diagnosis_date < DATEADD('year', -150, CURRENT_DATE()) THEN '{{placeholder_date}}' -- Unreasonably old dates
        ELSE DATE(ltcs.latest_diagnosis_date)
      END as latest_diagnosis_date
from inclusion_list il
inner join {{ ref('fct_person_ltc_summary')}} ltcs on il.olids_id = ltcs.person_id
