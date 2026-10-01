{{ config(materialized = 'table') }}

-- Latest description per emergency care attendance category code from the
-- UKHFD data dictionary SCD dimension.
select
    main_code_text as attendance_category_code
    , main_description as attendance_category_desc
from {{ ref('raw_ukhfd_emergency_care_attendance_category') }}
where is_latest = 1
qualify row_number() over (
    partition by main_code_text
    order by effective_from desc nulls last
) = 1
