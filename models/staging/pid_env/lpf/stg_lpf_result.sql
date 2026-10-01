{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'result']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_result') }}
),
cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y' and result_id is not null
),
deduplicated as (
    select *,
        row_number() over (
            partition by result_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
