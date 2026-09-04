select
    person_id,
    {{ consistent_sk_patient_id_format('pseudo_nhs_number')}}  as sk_patient_id
from {{ref('raw_csds_bridging')}}