select
    {{ dbt_utils.generate_surrogate_key(['person_id', 'provider_organisation_code']) }} as caseload_person_provider_id
    , person_id
    , max(sk_patient_id) as sk_patient_id
    , provider_organisation_code
    , max(provider_organisation_name) as provider_organisation_name
    , reporting_period_end_date as evidence_date
    , max(person_provider_period_id) as person_provider_period_id
    , count(*) as n_open_referrals
    , count_if(is_open_without_recorded_attendance) as n_open_referrals_without_attendance
    , count_if(first_recorded_attended_contact_date is not null) as n_open_referrals_with_attendance
    , min(referral_received_date) as earliest_open_referral_received_date
    , max(last_recorded_attended_contact_date) as last_attended_contact_on_open_referral_date
    , datediff(day, last_attended_contact_on_open_referral_date, evidence_date) as days_since_last_attended_contact
from {{ ref('fct_csds_current_caseload_referral') }}
where person_id is not null
group by person_id, provider_organisation_code, reporting_period_end_date
