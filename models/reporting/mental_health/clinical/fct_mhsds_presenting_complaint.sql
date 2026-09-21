with complaints as (
select
    clinical_record_id as presenting_complaint_id
    , person_id
    , sk_patient_id
    , referral_source_record_id
    , clinical_code as complaint_code
    , clinical_description as complaint_description
    , coding_scheme_code as finding_scheme_code
    , coding_scheme_description as finding_scheme_description
    , clinical_label_status
    , clinical_date as complaint_recorded_date
    , provider_organisation_code
    , provider_organisation_name
    , first_reported_period_end_date
    , last_reported_period_end_date
    , accepted_source_record_count
    , reporting_period_start_date
    , reporting_period_end_date
    , uniq_submission_id as submission_id
    , source_row_id
from {{ ref('fct_mhsds_clinical_record') }}
where clinical_record_type = 'presenting_complaint'

)
select
    c.*
    , iff(c.finding_scheme_code = '02', alternate_icd.description,
        iff(c.finding_scheme_code = '06', alternate_snomed.preferred_term, null))
        as alternative_diagnosis_scheme_code_description
    , case when c.finding_scheme_code = '02' and alternate_icd.code is not null then 'ICD-10'
        when c.finding_scheme_code = '06' and alternate_snomed.snomed_code is not null then 'SNOMED CT'
    end as alternative_diagnosis_coding_scheme
    , alternative_diagnosis_scheme_code_description is not null as has_alternative_diagnosis_scheme_match
from complaints as c
-- Finding and diagnosis schemes use different numbers. Candidate labels expose
-- possible source namespace errors; they never replace the declared-scheme label.
left join {{ ref('icd10_code') }} as alternate_icd
    on replace({{ clean_icd10_code('upper(trim(c.complaint_code))') }}, '.', '') = alternate_icd.code
    and c.finding_scheme_code = '02'
left join {{ ref('snomed_concept') }} as alternate_snomed
    on trim(c.complaint_code) = alternate_snomed.snomed_code and c.finding_scheme_code = '06'
