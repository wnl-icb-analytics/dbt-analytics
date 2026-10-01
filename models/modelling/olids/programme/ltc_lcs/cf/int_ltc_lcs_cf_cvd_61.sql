{{ config(materialized='table') }}

-- LTC LCS CVD case finding indicator CVD_61: high-risk CVD, QRISK >= 20, statin-naive
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-61.md
-- ICB_CF_CVD_61 = ICB_CF_CVD_61_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with "Cardiovascular disease high risk review declined" in the last 3 years.
-- Rule-1 declined marker name collides with the QRISK refset -> pin headline id 685fbf55 (1 code).

with woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("685fbf55-eda4-0f70-9bfc-3a0b55508944") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from woex
where person_id not in (select person_id from review_declined)
