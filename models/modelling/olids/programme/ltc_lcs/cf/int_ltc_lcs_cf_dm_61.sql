{{ config(materialized='table') }}

-- LTC LCS DM case finding indicator DM_61: latest HbA1c >= 48 and no diabetes diagnosis
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-61.md
-- ICB_CF_DM_61 = ICB_CF_DM_61_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with "High risk of diabetes mellitus annual review declined"
--   (1-code marker, name collides with the 7-code remission woEX set -> pinned id)
--   within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_61_woEX') }}
),

-- Rule 1: annual-review-declined recorded in the last 3 years
review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("877b18f5-9756-23a8-7a41-1900d12e70fa") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from review_declined)
