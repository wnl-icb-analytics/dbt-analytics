{{ config(materialized='view') }}

select
    source_record_id as group_therapy_contact_id
    , person_id
    , sk_patient_id
    , referral_source_record_id
    , uniq_care_cont_id
    , care_contact_date
    , care_contact_at
    , care_contact_time_precision
    , attendance_status_code
    , attendance_status_description
    , is_attended
    , is_dna
    , is_cancelled
    , clinical_contact_duration_minutes
    , group_therapy_code
    , group_therapy_description
    , patient_therapy_mode_code
    , patient_therapy_mode_description
    , case when patient_therapy_mode_code = '3' then 'patient_therapy_mode'
        else 'legacy_group_therapy_indicator' end as group_therapy_evidence_basis
    , coalesce((patient_therapy_mode_code = '3' and group_therapy_code = 'N')
        or (patient_therapy_mode_code in ('1','2') and group_therapy_code = 'Y'), false)
        as has_conflicting_group_therapy_evidence
    , provider_organisation_code
    , provider_organisation_name
    , service_or_team_id
    , service_or_team_type_code
    , service_or_team_type_description
    , treatment_site_code
    , treatment_site_name
    , reporting_period_start_date
    , reporting_period_end_date
from {{ ref('fct_mhsds_care_contact') }}
where patient_therapy_mode_code = '3' or group_therapy_code = 'Y'
