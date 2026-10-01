{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'appointments']
    )
}}

/*
Staging: LPF Appointments

Cleans and standardizes raw appointments data from local provider flows.
Removes deleted records and null appointment IDs.
Deduplicates by taking the latest version of each appointment record.

Source: raw_lpf_appointments
*/

with raw_data as (
    select * from {{ ref('raw_lpf_appointments') }}
),

cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y'
        and appointment_id is not null
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by appointment_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)

select * exclude (rn) from deduplicated where rn = 1
