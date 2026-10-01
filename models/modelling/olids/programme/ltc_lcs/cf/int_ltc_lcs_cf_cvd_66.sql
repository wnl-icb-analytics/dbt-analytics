{{ config(materialized='table') }}

-- LTC LCS CVD case finding indicator CVD_66: aged 75-84, not on a statin, no QRISK in last 5y
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-66.md
-- ICB_CF_CVD_66 = ICB_CF_CVD_66_woEX, then Rule 1 (final, exclude if matched):
--   "Cardiovascular disease high risk review declined" in the last 3 years.
-- Rule-1 declined marker -> pin headline id ae5b5d5d (1 code).

with woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_66_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("ae5b5d5d-a74a-a1b1-38db-071db479f3ca") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from woex
where person_id not in (select person_id from review_declined)
