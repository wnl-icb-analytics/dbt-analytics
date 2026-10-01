{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'diagnosis']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_diagnosis') }}
),
cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y' and diagnosis_id is not null
),
deduplicated as (
    select *,
        row_number() over (
            partition by diagnosis_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
