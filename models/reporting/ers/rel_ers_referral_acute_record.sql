{{ config(tags=['ers', 'large_periodic']) }}

with acute_records as (
    select
        'sus_outpatient' as acute_source,
        primarykey_id::varchar as acute_source_record_id,
        sk_patient_id as acute_sk_patient_id,
        unique_booking_reference_number as acute_booking_reference,
        appointment_date as acute_record_date
    from {{ ref('stg_sus_op_appointment') }}
    where nullif(trim(unique_booking_reference_number), '') is not null

    union all

    select
        'sus_admitted_patient' as acute_source,
        primarykey_id::varchar as acute_source_record_id,
        sk_patient_id as acute_sk_patient_id,
        unique_booking_reference_number as acute_booking_reference,
        spell_admission_date as acute_record_date
    from {{ ref('stg_sus_apc_spell') }}
    where nullif(trim(unique_booking_reference_number), '') is not null
)

select
    e.ubrn_id,
    a.acute_source,
    a.acute_source_record_id,
    a.acute_record_date,
    a.acute_booking_reference,
    e.normalised_ubrn,
    e.sk_patient_id as ers_sk_patient_id,
    a.acute_sk_patient_id,
    case
        when e.sk_patient_id is null or a.acute_sk_patient_id is null then 'missing_key'
        when e.sk_patient_id = a.acute_sk_patient_id then 'agree'
        else 'mismatch'
    end as patient_key_agreement
from acute_records as a
inner join {{ ref('fct_ers_referral') }} as e
    on nullif(replace(trim(a.acute_booking_reference), '-', ''), '') = e.normalised_ubrn
