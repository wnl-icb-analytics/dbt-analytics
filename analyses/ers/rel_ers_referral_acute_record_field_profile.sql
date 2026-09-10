-- Whole-table column coverage; no identifiers or patient records are returned.
with stats as (
    select object_construct(
        'ubrn_id', object_construct('populated', count(ubrn_id), 'blank', count_if(trim(ubrn_id::varchar) = '')),
        'acute_source', object_construct('populated', count(acute_source), 'blank', count_if(trim(acute_source::varchar) = '')),
        'acute_source_record_id', object_construct('populated', count(acute_source_record_id), 'blank', count_if(trim(acute_source_record_id::varchar) = '')),
        'acute_record_date', object_construct('populated', count(acute_record_date), 'blank', count_if(trim(acute_record_date::varchar) = '')),
        'acute_booking_reference', object_construct('populated', count(acute_booking_reference), 'blank', count_if(trim(acute_booking_reference::varchar) = '')),
        'normalised_ubrn', object_construct('populated', count(normalised_ubrn), 'blank', count_if(trim(normalised_ubrn::varchar) = '')),
        'ers_sk_patient_id', object_construct('populated', count(ers_sk_patient_id), 'blank', count_if(trim(ers_sk_patient_id::varchar) = '')),
        'acute_sk_patient_id', object_construct('populated', count(acute_sk_patient_id), 'blank', count_if(trim(acute_sk_patient_id::varchar) = '')),
        'patient_key_agreement', object_construct('populated', count(patient_key_agreement), 'blank', count_if(trim(patient_key_agreement::varchar) = ''))
    ) as fields
    from {{ ref('rel_ers_referral_acute_record') }}
)
select f.key as column_name, f.value as coverage
from stats, lateral flatten(input => fields) f
