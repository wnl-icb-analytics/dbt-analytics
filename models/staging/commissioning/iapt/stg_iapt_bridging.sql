-- Repeated bridge rows must not multiply facts. Conflicting usable keys remain unlinked.
select
    nullif(trim(person_id), '') as person_id
    , iff(
        count(distinct {{ consistent_sk_patient_id_format('nhs_number_pseudo') }}) = 1,
        max({{ consistent_sk_patient_id_format('nhs_number_pseudo') }}),
        null
    ) as sk_patient_id
from {{ ref('raw_iapt_bridging') }}
where nullif(trim(person_id), '') is not null
group by nullif(trim(person_id), '')
