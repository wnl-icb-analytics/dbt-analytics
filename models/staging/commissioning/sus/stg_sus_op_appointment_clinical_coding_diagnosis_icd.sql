{{
    config(materialized = 'view')
}}

select  {{ dbt_utils.generate_surrogate_key(["primarykey_id", "icd_id"]) }} as diagnosis_id 
            ,primarykey_id
            ,icd_id 
            ,rownumber_id 
            ,{{clean_icd10_code("code")}} as code
            ,present_on_admission
from {{ ref('raw_sus_op_appointment_clinical_coding_diagnosis_icd') }}
qualify row_number() over (partition by primarykey_id, icd_id, {{clean_icd10_code("code")}} order by rownumber_id) = 1
