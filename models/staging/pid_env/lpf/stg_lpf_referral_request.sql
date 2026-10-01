{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'referral']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_referral_request') }}
),
cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y' and referral_request_id is not null
),
deduplicated as (
    select *,
        row_number() over (
            partition by referral_request_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
