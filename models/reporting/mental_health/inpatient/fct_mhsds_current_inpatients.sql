select
    occupancy_interval_id
    , hospital_provider_spell_source_record_id
    , referral_source_record_id
    , person_id
    , sk_patient_id
    , provider_organisation_code
    , provider_organisation_name
    , admission_date
    , age_at_admission
    , source_derived_icb_commissioner_code
    , source_derived_icb_commissioner_name
    , last_submission_period_end_date
    , occupancy_evidence_as_of_date
    , calculation_date
    , current_date as duration_calculation_date
    , datediff(day, admission_date, current_date) as days_since_admission
    , occupancy_days_to_last_evidence
from {{ ref('fct_mhsds_inpatient_occupancy') }}
where is_current_inpatient
