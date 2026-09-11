{{
    config(materialized = 'view')
}}

select primarykey_id
    , {{ dbt_utils.generate_surrogate_key(['primarykey_id', 'coded_findings_id']) }} as source_record_id
    ,coded_findings_id
    ,rownumber_id
    ,code
from {{ ref('raw_sus_ecds_clinical_coded_findings') }}
