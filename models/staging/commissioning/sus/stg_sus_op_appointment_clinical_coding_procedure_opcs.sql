{{
    config(materialized = 'view')
}}

select primarykey_id
    ,opcs_id 
    ,rownumber_id
    ,code
from {{ ref('raw_sus_op_appointment_clinical_coding_procedure_opcs') }}