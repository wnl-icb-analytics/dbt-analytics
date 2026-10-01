{{
    config(materialized = 'view')
}}

select primarykey_id
    ,comorbidities_id
    ,rownumber_id
    ,code
from {{ ref('raw_sus_ecds_clinical_comorbidities') }}