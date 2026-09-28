with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_ward_stay_id', 's.start_date_home_leave', 's.start_time_home_leave', 'iff(s.uniq_ward_stay_id is null or s.start_date_home_leave is null, s.mhs509_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_ward_stay_id is null or s.start_date_home_leave is null as is_identity_incomplete
    from {{ ref('stg_mhsds_home_leave') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs509_uniq_id desc) = 1
)
select
    s.entity_id as home_leave_id
    , s.mhs509_uniq_id as source_row_id
    , 'MHS509' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_hosp_prov_spell_id as hospital_provider_spell_source_record_id
    , s.uniq_ward_stay_id as ward_stay_source_record_id
    , s.start_date_home_leave as leave_start_date
    , s.end_date_home_leave as leave_end_date
    , s.start_time_home_leave as leave_start_time
    , s.end_time_home_leave as leave_end_time
    , iff(s.end_date_home_leave >= s.start_date_home_leave, datediff(day, s.start_date_home_leave, s.end_date_home_leave), null) as recorded_elapsed_days
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
    , s.n_accepted_source_records
    , s.first_submission_period_end_date
    , s.is_identity_incomplete
from selected as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
