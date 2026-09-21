select
    clinical_record_id as diagnosis_record_id
    , person_id
    , sk_patient_id
    , referral_source_record_id
    , clinical_record_type as diagnosis_role
    , clinical_code as diagnosis_code
    , clinical_description as diagnosis_description
    , coding_scheme_code
    , coding_scheme_description
    , clinical_label_status
    , standardised_snomed_code
    , standardised_snomed_description
    , clinical_at as diagnosis_recorded_at
    , clinical_time_precision
    , clinical_time_basis
    , provider_organisation_code
    , provider_organisation_name
    , has_person_identifier_changed
    , first_reported_period_end_date
    , last_reported_period_end_date
    , accepted_source_record_count
    , source_table
    , source_row_id
from {{ ref('fct_mhsds_clinical_record') }}
where clinical_record_type in (
    'previous_diagnosis', 'provisional_diagnosis', 'primary_diagnosis', 'secondary_diagnosis'
)
