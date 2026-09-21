select
    s.mhs008_uniq_id as care_plan_period_id
    , s.mhs008_uniq_id as source_row_id
    , 'MHS008' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_care_plan_id as care_plan_source_id
    , s.care_plan_type_mh as care_plan_type_code
    , s.care_plan_creat_date as creation_date
    , s.care_plan_creation_time as creation_time
    , s.care_plan_last_update_date as last_update_date
    , s.care_plan_last_update_time as last_update_time
    , s.care_plan_implement_date as implementation_date
    , plan_label.description as care_plan_type_description
    , {{ dbt_utils.generate_surrogate_key(['s.person_id', 's.org_id_prov', 's.reporting_period_end_date']) }} as person_provider_period_id
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_care_plan') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as plan_label
    on upper(trim(s.care_plan_type_mh::varchar)) = plan_label.code and plan_label.code_set_name = 'care_plan_type'
