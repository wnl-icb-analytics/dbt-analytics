{{
    config(
        materialized = 'view',
        tags = ['sdl', 'community_pld', 'ref']
    )
}}

-- Community Referrals, current provider statement only: stg_ref restricted to
-- the winning file per (provider, FY, month) via
-- stg_community_pld_latest_submission. Sum-safe; cumulative restatements
-- excluded. Full submission history is in stg_ref. Rows with no resolvable
-- period are not included (they cannot be assigned to a slice).

select s.*
from {{ ref('stg_ref') }} as s
join {{ ref('stg_community_pld_latest_submission') }} as l
    on l.feed = 'REF'
   and l.meta_file_id = s.meta_file_id
   and l.meta_batch_id = s.meta_batch_id
   and equal_null(l.dv_provider_code, s.provider_code)
   and equal_null(l.dv_financial_year, s.dv_financial_year)
   and equal_null(l.dv_financial_month, s.dv_financial_month)
