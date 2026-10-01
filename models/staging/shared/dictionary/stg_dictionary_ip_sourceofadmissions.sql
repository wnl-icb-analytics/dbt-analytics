{{
    config(materialized = 'table')
}}

select
    sk_source_of_admission_id,
    bk_source_of_admission_code,
    source_of_admission_name,
    source_of_admission_full_name
from {{ ref('raw_dictionary_ip_sourceofadmissions') }}
qualify row_number() over (
    partition by sk_source_of_admission_id 
    order by date_updated desc
    ) = 1