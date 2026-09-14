{{ config(materialized = 'table') }}

-- Latest description per emergency care department type code from UKHFD.
select
    main_code_text as department_type
    , main_description as department_type_desc
from {{ ref('raw_ukhfd_emergency_care_department_type') }}
where is_latest = 1
qualify row_number() over (
    partition by main_code_text
    order by effective_from desc nulls last
) = 1
