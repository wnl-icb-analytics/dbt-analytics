select 'referral' as source_entity, count(*) as row_count,
    count_if(person_id is null) as missing_person,
    count_if(sk_patient_id is null) as missing_patient_bridge,
    count_if(referral_received_date < '2021-04-01') as before_legacy_cutoff,
    count_if(referral_received_time_precision = 'date') as date_only_rows,
    null as missing_parent, null as person_conflicting_parent
from {{ ref('fct_csds_referral') }}
union all
select 'contact', count(*), count_if(person_id is null), count_if(sk_patient_id is null),
    count_if(care_contact_date < '2021-04-01'), count_if(care_contact_time_precision = 'date'),
    count_if(not is_referral_linked), count_if(not is_referral_person_consistent)
from {{ ref('fct_csds_care_contact') }}
