{{
    config(
        materialized = 'table',
        tags=['ers']
        )
    }}
select
    person_id,
    {{ consistent_sk_patient_id_format('nhs_number_pseudo') }} as sk_patient_id
from {{ ref('raw_ers_pc_bridging') }}
