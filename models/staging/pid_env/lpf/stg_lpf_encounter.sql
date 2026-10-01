{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'encounter']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_encounter') }}
),
cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y' and encounter_id is not null
),
deduplicated as (
    select *,
        row_number() over (
            partition by encounter_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)
select * exclude (rn) from deduplicated where rn = 1
