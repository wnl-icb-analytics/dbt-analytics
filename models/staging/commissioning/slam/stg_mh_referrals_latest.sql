{{
    config(
        materialized = 'view',
        tags = ['sdl', 'mh_slam', 'mh_referrals']
    )
}}

-- MH Referrals, current provider statement only: stg_mh_referrals restricted
-- to the winning file per (provider, commissioner, FY, month) via
-- stg_mh_slam_latest_submission. The cumulative Flex/Freeze restatements are
-- excluded. Full submission history is in stg_mh_referrals. Rows with no
-- resolvable period are not included (they cannot be assigned to a slice).
--
-- Grain: referral x active month (monthly caseload snapshot) — an open
-- referral has one row for every month it is active, so counting rows
-- overcounts referrals ~4x. dv_is_current_referral_row marks the most recent
-- snapshot row per (provider, referral): filter on it to count referrals or
-- read their latest state (closure fields populate on the final rows).
-- Rows with a NULL referral_id (none observed) are all flagged current, as
-- they cannot be grouped.

with latest as (
    select s.*
    from {{ ref('stg_mh_referrals') }} as s
    join {{ ref('stg_mh_slam_latest_submission') }} as l
        on l.dataset = 'MH_REFERRALS'
       and l.meta_file_id = s.meta_file_id
       and l.meta_batch_id = s.meta_batch_id
       and equal_null(l.dv_provider_code, s.provider_code)
       and equal_null(l.dv_commissioner, coalesce(s.commissioner_code, s.dlp_commissioner_code))
       and equal_null(l.dv_financial_year, s.dv_financial_year)
       and equal_null(l.dv_financial_month, s.dv_financial_month)
)

select
    *,
    referral_id is null
    or row_number() over (
        partition by provider_code, referral_id
        order by dv_financial_year desc, dv_financial_month desc,
                 extract_date desc nulls last, meta_sk_row_id desc
    ) = 1                                       as dv_is_current_referral_row
from latest
