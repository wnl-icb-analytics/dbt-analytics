{{
    config(
        materialized = 'table',
        tags=['mhsds']
        )
}}
-- Ward details join to MHS502 by provider, submission and ward code.
with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs903warddetails')) }}
)

select
    ward_code
    , site_id_of_ward
    , ward_intended_sex
    , ward_intended_clin_care_mh
    , ward_age
    , ward_type
    , ward_sec_level
    , locked_ward_ind
    , avail_bed_days
    , closed_bed_days
    , mhs903_uniq_id
    , org_id_prov
    , uniq_submission_id
    , uniq_month_id
    , effective_from
    , uniq_ward_code
    , row_number
    , dmic_import_log_id
    , dmic_system_id
    , dmic_ccg_code
    , dmic_date_added
    , file_type
    , reporting_period_start_date
    , reporting_period_end_date
    , dmic_dataset
from accepted_records
qualify row_number() over (
    partition by org_id_prov, uniq_submission_id, ward_code
    order by row_number desc, mhs903_uniq_id desc
) = 1
