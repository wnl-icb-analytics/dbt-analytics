{{ config(materialized='table', tags=['mhsds']) }}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs901staffdetails')) }}
)

, ranked as (
    select
        s.*
        , count(*) over (
            partition by s.uniq_submission_id, s.uniq_care_prof_local_id
        ) as source_record_count
        , row_number() over (
            partition by s.uniq_submission_id, s.uniq_care_prof_local_id
            order by
                try_to_number(s.row_number) desc nulls last
                , s.mhs901_uniq_id desc
        ) as source_record_rank
    from accepted_records as s
)

select
    s.mhs901_uniq_id
    , s.uniq_care_prof_local_id
    , s.care_prof_local_id
    , s.org_id_care_prof_local_id
    , s.prof_reg_body_code
    , s.care_prof_staff_gp_mh as staff_group_code
    , s.main_spec_code_mh as main_specialty_code
    , s.occ_code as occupation_code
    , s.care_prof_job_role_code as job_role_code
    , s.org_id_prov
    , s.row_number as source_row_number
    , s.source_record_count
    , s.source_record_count > 1 as has_duplicate_source_records
    , s.uniq_submission_id
    , s.uniq_month_id
    , s.reporting_period_start_date::date as reporting_period_start_date
    , s.reporting_period_end_date::date as reporting_period_end_date
    , s.dmic_dataset as mhsds_version
    , s.effective_from as source_file_received_at
    , s.dmic_date_added as source_loaded_at
from ranked as s
where s.source_record_rank = 1
