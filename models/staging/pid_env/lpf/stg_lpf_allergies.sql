{{
    config(
        materialized='view',
        tags=['staging', 'pid_env', 'allergies']
    )
}}

/*
Staging: LPF Allergies

Cleans and standardizes raw allergies data from local provider flows.
Removes deleted records and null allergy IDs.
Deduplicates by taking the latest version of each allergy record.

Source: raw_lpf_allergies
*/

with raw_allergies as (
    select * from {{ ref('raw_lpf_allergies') }}
),

cleaned as (
    -- Remove deleted records and null allergy IDs
    select
        metadata_file_path,
        metadata_file_row_number,
        metadata_record_ingestion_timestamp,
        metadata_file_content_key,
        metadata_file_last_modified,
        allergy_id,
        version,
        person_id,
        encounter_id,
        allergen_code_id,
        allergen_code_system_id,
        allergen_display,
        onset_date,
        resolved_date,
        reaction_code_id,
        reaction_code_system_id,
        reaction_display,
        severity_code_id,
        severity_code_system_id,
        severity_display,
        status_code_id,
        status_code_system_id,
        status_display,
        category_code_id,
        category_code_system_id,
        category_display,
        type_code_id,
        type_code_system_id,
        type_display,
        criticality_code_id,
        criticality_code_system_id,
        criticality_display,
        asserted_date
    from raw_allergies
    where coalesce(delete_ind, 'N') != 'Y'
        and allergy_id is not null
),

deduplicated as (
    -- Take the latest version of each allergy
    select
        *,
        row_number() over (
            partition by allergy_id
            order by version desc, metadata_record_ingestion_timestamp desc, metadata_file_row_number desc
        ) as rn
    from cleaned
)

select
    metadata_file_path,
    metadata_file_row_number,
    metadata_record_ingestion_timestamp,
    metadata_file_content_key,
    metadata_file_last_modified,
    allergy_id,
    version,
    person_id,
    encounter_id,
    allergen_code_id,
    allergen_code_system_id,
    allergen_display,
    onset_date,
    resolved_date,
    reaction_code_id,
    reaction_code_system_id,
    reaction_display,
    severity_code_id,
    severity_code_system_id,
    severity_display,
    status_code_id,
    status_code_system_id,
    status_display,
    category_code_id,
    category_code_system_id,
    category_display,
    type_code_id,
    type_code_system_id,
    type_display,
    criticality_code_id,
    criticality_code_system_id,
    criticality_display,
    asserted_date
from deduplicated
where rn = 1
