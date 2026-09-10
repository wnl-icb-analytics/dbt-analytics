{{ config(materialized='table', tags=['mhsds']) }}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs202careactivity')) }}
)

select
    a.mhs202_uniq_id
    , a.uniq_care_act_id
    , a.care_act_id
    , a.uniq_care_cont_id
    , a.care_contact_id
    , a.uniq_serv_req_id
    , a.person_id
    , a.uniq_care_prof_local_id
    , a.care_prof_local_id
    , a.clin_contact_dur_of_care_act as clinical_contact_duration_minutes
    -- ETOS v6 replaced the three legacy clinical-code columns and the unit field.
    , iff(a.dmic_dataset = 'V6', a.procedure, a.code_proc_and_proc_status)
        as procedure_code
    , a.find_scheme_in_use as finding_scheme_code
    , iff(a.dmic_dataset = 'V6', a.finding, a.code_find) as finding_code
    , a.master_snomed_ct_finding_code as standardised_snomed_finding_code
    -- V4.1 removed the scheme field when observations became SNOMED CT-only.
    , coalesce(
        (a.dmic_dataset = 'V6' and a.observation is not null)
        or (a.dmic_dataset = 'V5' and a.code_obs is not null)
        or (a.dmic_dataset = 'V4'
            and a.reporting_period_start_date >= '2020-04-01'
            and a.code_obs is not null)
        , false
    ) as is_observation_scheme_inferred
    , iff(is_observation_scheme_inferred, '03', a.obs_scheme_in_use) as observation_scheme_code
    , iff(a.dmic_dataset = 'V6', a.observation, a.code_obs) as observation_code
    , a.master_snomed_ct_obs_code as standardised_snomed_observation_code
    , a.obs_value as observation_value
    , iff(a.dmic_dataset = 'V6', a.unit_of_measurement_ucum, a.unit_measure)
        as unit_of_measurement_code
    , a.org_id_prov
    , a.record_number as source_record_number
    , a.row_number as source_row_number
    , a.uniq_submission_id
    , a.uniq_month_id
    , a.reporting_period_start_date::date as reporting_period_start_date
    , a.reporting_period_end_date::date as reporting_period_end_date
    , a.dmic_dataset as mhsds_version
    , a.effective_from as source_file_received_at
    , a.dmic_date_added as source_loaded_at
from accepted_records as a
