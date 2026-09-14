{{
    config(materialized = 'view')
}}

select primarykey_id
    ,snomed_id
    ,rownumber_id
    ,code
    ,date::date as date
    ,time::time as time
from {{ ref('raw_sus_ecds_clinical_investigations_snomed') }}