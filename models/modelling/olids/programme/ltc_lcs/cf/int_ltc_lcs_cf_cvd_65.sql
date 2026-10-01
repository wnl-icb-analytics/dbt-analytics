{{ config(materialized='table') }}

-- LTC LCS CVD case finding indicator CVD_65: QRISK >= 10 on a statin, not high-intensity
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-65.md
-- ICB_CF_CVD_65 = ICB_CF_CVD_65_woEX, then Rule 1 (final, exclude if matched):
--   "Cardiovascular disease high risk review declined" in the last 3 years.
-- Rule-1 declined marker name collides with the 19-code lipid-AR woEX set -> pin headline id
--   e291831b (1 code).

with woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_65_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("e291831b-466c-ed7a-bba4-eeef6449a8f6") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from woex
where person_id not in (select person_id from review_declined)
