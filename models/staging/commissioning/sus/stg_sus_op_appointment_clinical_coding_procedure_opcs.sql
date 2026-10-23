{{
    config(materialized = 'view')
}}

select primarykey_id
    , {{ dbt_utils.generate_surrogate_key(['primarykey_id', 'opcs_id']) }} as procedure_id
    , try_to_date(date::varchar) as procedure_date
    ,opcs_id 
    ,rownumber_id
    ,code
from {{ ref('raw_sus_op_appointment_clinical_coding_procedure_opcs') }}
