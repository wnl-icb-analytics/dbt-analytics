{{ config(materialized='ephemeral') }}

select *
from {{ ref('stg_ukhfd_ecds_etos_code_sets') }}
qualify row_number() over (
    partition by code_set_name, snomed_code, etos_version
    order by
        -- v4.0.6 was loaded from two files; keep the UnmergedCells variant,
        -- which alone carries search terms.
        source_file_name ilike '%UnmergedCells%' desc,
        source_file_name,
        -- v4 and v4.0.5 list one chief complaint both live and deprecated.
        is_deprecated,
        source_unique_key
) = 1
