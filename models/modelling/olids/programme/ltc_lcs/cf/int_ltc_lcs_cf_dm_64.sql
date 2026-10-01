{{ config(materialized='table') }}

-- LTC LCS DM case finding indicator DM_64: latest BMI >= 35 (>= 32.5 BAME), no HbA1c in 2y
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-64.md
-- ICB_CF_DM_64 = ICB_CF_DM_64_woEX, then Rule 1 (final, exclude if matched):
--   exclude "annual review declined" (1-code marker, pinned id) within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_64_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("4ed7513e-8c5c-68c8-3850-f4bff1387f79") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from review_declined)
