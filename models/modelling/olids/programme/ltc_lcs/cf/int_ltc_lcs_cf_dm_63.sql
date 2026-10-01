{{ config(materialized='table') }}

-- LTC LCS DM case finding indicator DM_63: latest HbA1c 46-47, no HbA1c in the last year
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-63.md
-- ICB_CF_DM_63 = ICB_CF_DM_63_woEX, then Rule 1 (final, exclude if matched):
--   exclude "annual review declined" (1-code marker, pinned id) within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_63_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("776f1587-24f9-1d71-a7ff-6ff4e2a1c27d") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from review_declined)
