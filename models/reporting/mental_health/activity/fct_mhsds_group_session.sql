select
    s.mhs301_uniq_id as group_session_id
    , s.mhs301_uniq_id as source_row_id
    , 'MHS301' as source_table
    , s.uniq_group_sess_id as group_session_source_id
    , s.group_sess_date as group_session_date
    , s.group_sess_type as group_session_type_code
    , s.clin_cont_dur_of_group_sess as session_duration_minutes
    , s.number_of_group_sess_particip as recorded_participant_count
    , s.site_id_of_treat as treatment_site_code
    , s.serv_team_type_ref_to_mh as service_or_team_type_code
    , s.uniq_care_prof_local_id as lead_professional_id
    , s.org_id_comm as commissioner_organisation_code
    , session_label.description as group_session_type_description
    , team.description as service_or_team_type_description
    , s.group_sess_date between s.reporting_period_start_date and s.reporting_period_end_date as is_session_date_in_reporting_period
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
    , treatment_site.organisation_name as treatment_site_name
    , commissioner.organisation_name as commissioner_organisation_name
from {{ ref('stg_mhsds_group_session') }} as s
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as session_label
    on upper(trim(s.group_sess_type::varchar)) = session_label.code and session_label.code_set_name = 'group_session_type'
left join {{ ref('mhsds_service_or_team_type') }} as team on s.serv_team_type_ref_to_mh = team.code
left join {{ ref('int_mhsds_organisation') }} as treatment_site
    on upper(s.site_id_of_treat) = upper(treatment_site.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner
    on upper(s.org_id_comm) = upper(commissioner.organisation_code)
