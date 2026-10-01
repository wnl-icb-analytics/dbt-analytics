{{ config(materialized='table') }}

-- LTC LCS DM case finding indicator DM_65: latest BMI >= 30 (>= 27.5 BAME), no HbA1c in 2y
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-65.md
-- ICB_CF_DM_65 = ICB_CF_DM_65_woEX, then Rule 1 (final, exclude if matched):
--   exclude "annual review declined" (1-code marker, pinned id) within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_65_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("49bd264d-50de-3a1f-09e3-c06db976f89b") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from review_declined)
