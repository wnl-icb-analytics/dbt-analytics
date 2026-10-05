select
    s.mhs302_uniq_id as drop_in_contact_id
    , s.mhs302_uniq_id as source_row_id
    , 'MHS302' as source_table
    , s.uniq_mh_drop_in_contact_id as drop_in_contact_source_id
    , s.care_contact_date_mh_drop_in_contact as contact_date
    , s.start_time_drop_in_contact as contact_start_time
    , s.mh_drop_in_contact_outcome as contact_outcome_code
    , s.gender_id_code as gender_identity_code
    , s.cons_mechanism_mh as consultation_mechanism_code
    , s.age_rep_period_end as age_at_period_end
    , s.ethnic_category as ethnicity_2001_code
    , outcome_label.description as contact_outcome_description
    , gender_label.description as gender_identity_description
    , ethnic.ethnicity_2001_detailed_description as ethnicity_2001_description
    , mechanism.description as consultation_mechanism_description
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_drop_in_contact') }} as s
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as outcome_label
    on upper(trim(s.mh_drop_in_contact_outcome::varchar)) = outcome_label.code and outcome_label.code_set_name = 'drop_in_outcome'
left join {{ ref('mhsds_domain_code_lookup') }} as gender_label
    on upper(trim(s.gender_id_code::varchar)) = gender_label.code and gender_label.code_set_name = 'gender_identity'
left join {{ ref('nhs_ethnicity_2001') }} as ethnic on s.ethnic_category = ethnic.ethnicity_2001_code
left join {{ ref('consultation_mechanism') }} as mechanism on s.cons_mechanism_mh = mechanism.code
