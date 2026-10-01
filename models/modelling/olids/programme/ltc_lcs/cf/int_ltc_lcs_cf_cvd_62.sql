{{ config(materialized='table') }}

-- LTC LCS CVD case finding indicator CVD_62: high-risk CVD, QRISK 15 to <20, statin-naive
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-62.md
-- ICB_CF_CVD_62 = ICB_CF_CVD_62_woEX (mutually exclusive from 61_woEX), then Rule 1
--   (final, exclude if matched): "Cardiovascular disease high risk review declined" in last 3y.
-- Rule-1 declined marker -> pin headline id 68d3e80c (1 code).

with woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}
),

review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("68d3e80c-08d3-7134-0eb7-4b53b0a59b4b") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from woex
where person_id not in (select person_id from review_declined)
