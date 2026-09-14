{{
    config(
        materialized = 'view',
        tags = ['mhcorl', 'sdl', 'mh']
    )
}}

-- MHCORL, current provider statement only: stg_mhcorl restricted to the
-- winning file per (dataset, provider, commissioner, FY, month) via
-- stg_mhcorl_latest_submission. Sum-safe; the cumulative Flex/Freeze,
-- year-to-date and per-commissioner restatements are excluded. Full
-- submission history is in stg_mhcorl. Rows with no resolvable period are
-- not included (they cannot be assigned to a slice).

select s.*
from {{ ref('stg_mhcorl') }} as s
join {{ ref('stg_mhcorl_latest_submission') }} as l
    on l.meta_file_id = s.meta_file_id
   and l.meta_batch_id = s.meta_batch_id
   and equal_null(l.dv_dataset, s.dv_dataset)
   and equal_null(l.dv_provider_code, s.provider_code)
   and equal_null(l.dv_commissioner, coalesce(s.commissioner_code, s.dlp_commissioner_code))
   and equal_null(l.dv_financial_year, s.dv_financial_year)
   and equal_null(l.dv_financial_month, s.dv_financial_month)
