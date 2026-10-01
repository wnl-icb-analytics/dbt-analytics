{{ config(materialized='table') }}

-- LTC LCS CVD case finding indicator CVD_64: QRISK >= 10 and NOT on a statin
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-64.md
-- ICB_CF_CVD_64 = ICB_CF_CVD_64_woEX, then Rule 1 (final, exclude if matched):
--   "Cardiovascular disease high risk review declined" in the last 3 years.
-- Rule-1 declined marker name collides with the 845-code STAT_COD woEX set -> pin headline id
--   20072ac6 (1 code). Pinning the headline id is critical here: the name would otherwise union
--   845 STAT_COD codes and catastrophically over-exclude.

with woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_64_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("20072ac6-a53f-00e5-6e47-a40a38a59995") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from woex
where person_id not in (select person_id from review_declined)
