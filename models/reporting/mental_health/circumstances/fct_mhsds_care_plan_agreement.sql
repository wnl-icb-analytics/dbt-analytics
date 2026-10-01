select
    s.mhs009_uniq_id as care_plan_agreement_id
    , s.mhs009_uniq_id as source_row_id
    , 'MHS009' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_care_plan_id as care_plan_source_id
    , s.family_care_plan_indicator as family_involvement_code
    , s.no_family_care_plan_reason as family_exclusion_reason_code
    , s.care_plan_content_agreed_by as legacy_content_agreed_by_code
    , s.care_plan_agreed_by as agreed_by_code
    , s.care_plan_content_agreed_date as legacy_content_agreed_date
    , s.care_plan_agreed_date as agreement_date
    , family_label.description as family_involvement_description
    , reason_label.description as family_exclusion_reason_description
    , legacy_label.description as legacy_content_agreed_by_description
    , agreed_label.description as agreed_by_description
    , p.care_plan_period_id
    , p.care_plan_period_id is not null as is_same_submission_care_plan_linked
    , s.person_id = p.person_id as is_care_plan_person_consistent
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_care_plan_agreement') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as family_label
    on upper(trim(s.family_care_plan_indicator::varchar)) = family_label.code and family_label.code_set_name = 'family_care_plan_involvement'
left join {{ ref('mhsds_domain_code_lookup') }} as reason_label
    on upper(trim(s.no_family_care_plan_reason::varchar)) = reason_label.code and reason_label.code_set_name = 'family_care_plan_exclusion_reason'
left join {{ ref('mhsds_domain_code_lookup') }} as legacy_label
    on upper(trim(s.care_plan_content_agreed_by::varchar)) = legacy_label.code and legacy_label.code_set_name = 'care_plan_content_agreed_by'
left join {{ ref('mhsds_domain_code_lookup') }} as agreed_label
    on upper(trim(s.care_plan_agreed_by::varchar)) = agreed_label.code and agreed_label.code_set_name = 'care_plan_agreed_by'
left join {{ ref('fct_mhsds_care_plan_period') }} as p
    on s.uniq_submission_id = p.submission_id and s.uniq_care_plan_id = p.care_plan_source_id
