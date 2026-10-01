{{
    config(
        materialized = 'view',
        tags = ['sdl', 'mh_slam', 'mh_apc']
    )
}}

-- MH APC, current provider statement only: stg_mh_apc restricted to the
-- winning file per (provider, commissioner, FY, month) via
-- stg_mh_slam_latest_submission.
-- Sum-safe (occupied_bed_days); the cumulative Flex/Freeze restatements are
-- excluded. Full submission history is in stg_mh_apc. Rows with no resolvable
-- period are not included (they cannot be assigned to a slice).

select s.*
from {{ ref('stg_mh_apc') }} as s
join {{ ref('stg_mh_slam_latest_submission') }} as l
    on l.dataset = 'MH_APC'
   and l.meta_file_id = s.meta_file_id
   and l.meta_batch_id = s.meta_batch_id
   and equal_null(l.dv_provider_code, s.provider_code)
   and equal_null(l.dv_commissioner, coalesce(s.commissioner_code, s.dlp_commissioner_code))
   and equal_null(l.dv_financial_year, s.dv_financial_year)
   and equal_null(l.dv_financial_month, s.dv_financial_month)
