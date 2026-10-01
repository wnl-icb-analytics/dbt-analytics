{{ config(materialized = 'table') }}

-- Latest description per Mental Health Act legal status classification code
-- from the UKHFD data dictionary SCD dimension.
select
    main_code_text as legal_status_code
    , main_description as legal_status_desc
from {{ ref('raw_ukhfd_mental_health_act_legal_status_classification') }}
where is_latest = 1
qualify row_number() over (
    partition by main_code_text
    order by effective_from desc nulls last
) = 1
