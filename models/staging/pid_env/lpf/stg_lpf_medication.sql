{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'medication']
    )
}}
with raw_data as (
    select * from {{ ref('raw_lpf_medication') }}
),
cleaned as (
    select * from raw_data
    where coalesce(delete_ind, 'N') != 'Y' and medication_id is not null
),
deduplicated as (
    select *,
        row_number() over (
            partition by medication_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)

select  medication_id
    , {{ clean_organisation_id("left(split_part(metadata_file_path, '/', 1), 5)") }} as organisation_id
    -- encounter details
    , encounter_id
    , {{ consistent_sk_patient_id_format('person_id') }} as sk_patient_id
    , prescribing_provider_id
    , to_date(start_date) as start_date
    , to_date(stop_date) as end_date
    -- drug
    , drug_code_id
    , drug_code_system_id -- TO DO: request populated or add by provider case when e.g., RAL == CDC's NCHS discontinued system?
    , drug_display
    -- administration information
    , dose_quantity_unit_code_id
    , dose_quantity_unit_code_system_id
    , dose_quantity_unit_display
    , dose_volume_amount
    , dose_volume_unit_code_id
    , dose_volume_unit_code_system_id
    , dose_volume_unit_display
    , route_code_id
    , route_code_system_id -- TO DO: request populated 
    , route_display
    -- order outcome
    , order_status_code_id
    , order_status_code_system_id
    , order_status_display
    , order_detail_line

    -- Not currently populated administrative information kept to review as more providers are involved
    , frequency_code_id
    , frequency_code_system_id
    , frequency_display
    , dose_strength_amount
    , dose_strength_unit_code_id
    , dose_strength_unit_code_system_id
    , dose_strength_unit_display


from deduplicated where rn = 1


