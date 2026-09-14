with legacy_relationships as (
    select
        a.mhs202_uniq_id as source_relationship_id
        , 'MHS202' as source_table
        , 'legacy_direct' as relationship_source
        , a.uniq_care_act_id
        , a.uniq_care_prof_local_id
        , a.uniq_serv_req_id
        , a.uniq_care_cont_id
        , a.person_id
        , a.org_id_prov
        , a.uniq_submission_id
        , a.uniq_month_id
        , a.reporting_period_start_date
        , a.reporting_period_end_date
        , a.mhsds_version
        , a.source_file_received_at
        , a.source_loaded_at
    from {{ ref('stg_mhsds_care_activity') }} as a
    where a.mhsds_version <> 'V6'
        and a.uniq_care_prof_local_id is not null
)

, v6_relationships as (
    select
        s.mhs206_uniq_id as source_relationship_id
        , 'MHS206' as source_table
        , 'activity_staff_relationship' as relationship_source
        , s.uniq_care_act_id
        , s.uniq_care_prof_local_id
        , s.uniq_serv_req_id
        , s.uniq_care_cont_id
        , s.person_id
        , s.org_id_prov
        , s.uniq_submission_id
        , s.uniq_month_id
        , s.reporting_period_start_date
        , s.reporting_period_end_date
        , s.mhsds_version
        , s.source_file_received_at
        , s.source_loaded_at
    from {{ ref('stg_mhsds_staff_activity') }} as s
)

, relationships as (
    select * from legacy_relationships
    union all
    select * from v6_relationships
)

select
    {{ dbt_utils.generate_surrogate_key([
        'r.source_table',
        'r.source_relationship_id'
    ]) }} as care_activity_staff_relationship_id
    , {{ dbt_utils.generate_surrogate_key([
        'r.org_id_prov',
        'r.reporting_period_end_date',
        'r.uniq_care_act_id'
    ]) }} as care_activity_source_record_id
    , {{ dbt_utils.generate_surrogate_key([
        'r.org_id_prov',
        'r.reporting_period_end_date',
        'r.uniq_care_prof_local_id'
    ]) }} as care_professional_period_id
    , 'MHSDS' as source_dataset
    , r.source_relationship_id
    , r.source_table
    , r.relationship_source
    , r.uniq_care_act_id
    , r.uniq_care_prof_local_id
    , r.uniq_serv_req_id as referral_source_record_id
    , r.uniq_care_cont_id
    , r.person_id
    , p.staff_group_code
    , p.staff_group_description
    , p.staff_group_label_status
    , p.main_specialty_code
    , p.main_specialty_description
    , p.main_specialty_label_status
    , p.occupation_code
    , p.occupation_description
    , p.occupation_label_status
    , p.job_role_code
    , p.job_role_description
    , p.job_role_label_status
    , r.org_id_prov as provider_organisation_code
    , coalesce(p.provider_organisation_name, provider.organisation_name)
        as provider_organisation_name
    , a.source_record_id is not null as is_care_activity_linked
    , p.care_professional_period_id is not null as is_professional_detail_linked
    , r.uniq_submission_id
    , r.uniq_month_id
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.mhsds_version
    , r.source_file_received_at
    , r.source_loaded_at
from relationships as r
left join {{ ref('fct_mhsds_care_activity') }} as a
    on r.uniq_submission_id = a.uniq_submission_id
    and r.uniq_care_act_id = a.uniq_care_act_id
left join {{ ref('dim_mhsds_care_professional_period') }} as p
    on r.uniq_submission_id = p.uniq_submission_id
    and r.uniq_care_prof_local_id = p.uniq_care_prof_local_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(r.org_id_prov) = upper(provider.organisation_code)
