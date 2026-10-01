{{ config(materialized='table') }}

-- LTC LCS CVD case finding indicator CVD_63: QRISK >= 10 on a statin with non-HDL > 2.5
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-63.md
-- ICB_CF_CVD_63 = ICB_CF_CVD_63_woEX, then Rule 1 (final, exclude if matched):
--   "Cardiovascular disease high risk review declined" in the last 3 years.
-- Rule-1 declined marker name collides with the 4-code non-HDL woEX set -> pin headline id
--   a318069f (1 code).

with woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("a318069f-408c-4436-fbaa-070018f2db3a") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from woex
where person_id not in (select person_id from review_declined)
