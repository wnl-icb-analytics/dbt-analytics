select
    s.mhs004_uniq_id as employment_observation_id
    , s.mhs004_uniq_id as source_row_id
    , 'MHS004' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.employ_status as employment_status_code
    , s.employ_status_start_date as employment_status_start_date
    , s.employ_status_end_date as employment_status_end_date
    , s.employ_status_rec_date as employment_status_recorded_date
    , s.pat_prim_emp_cont_type_mh as employment_contract_type_code
    , s.week_hours_worked as weekly_hours_worked_code
    , hours_label.description as weekly_hours_worked_description
    , status_label.description as employment_status_description
    , case when nullif(trim(s.employ_status), '') is null then 'code_missing'
        when status_label.description is not null then 'labelled' else 'code_unmatched' end as employment_status_label_status
    , contract_label.description as employment_contract_type_description
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_employment') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as status_label
    on upper(trim(s.employ_status::varchar)) = status_label.code and status_label.code_set_name = 'employment_status'
left join {{ ref('mhsds_domain_code_lookup') }} as contract_label
    on upper(trim(s.pat_prim_emp_cont_type_mh::varchar)) = contract_label.code and contract_label.code_set_name = 'employment_contract_type'
left join {{ ref('mhsds_domain_code_lookup') }} as hours_label
    on upper(trim(s.week_hours_worked::varchar)) = hours_label.code and hours_label.code_set_name = 'weekly_hours_worked'
