{{ config(materialized = 'table') }}

-- Latest description per urgent and emergency care activity type code from
-- the UKHFD data dictionary SCD dimension.
select
    main_code_text as uec_activity_type_code
    , main_description as uec_activity_type_desc
from {{ ref('raw_ukhfd_uec_activity_type') }}
where is_latest = 1
qualify row_number() over (
    partition by main_code_text
    order by effective_from desc nulls last
) = 1
