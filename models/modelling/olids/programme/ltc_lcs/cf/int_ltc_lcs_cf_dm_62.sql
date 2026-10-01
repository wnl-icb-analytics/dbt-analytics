{{ config(materialized='table') }}

-- LTC LCS DM case finding indicator DM_62: gestational diabetes, no HbA1c in the last year
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-62.md
-- ICB_CF_DM_62 = ICB_CF_DM_62_woEX, then Rule 1 (final, exclude if matched):
--   exclude "annual review declined" (1-code marker, pinned id) within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_62_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("5d6cf960-e4cd-9015-fd93-5105d7bf219a") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from review_declined)
