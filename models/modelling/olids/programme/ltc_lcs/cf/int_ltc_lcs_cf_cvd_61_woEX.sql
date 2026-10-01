{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_61_woEX population (pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-61.md
--
-- CVD_61_woEX = CVD_61_BASE, then INCLUDE who match QRISK refset (999011011000230107)
--   latest numeric value >= 20.
-- QRISK valueset name people_at_high_risk_cvd_eligible_pop_with_qrisk20_vs1 COLLIDES with the
-- 1-code Rule-1 declined marker -> pin the woEX QRISK-refset id 89e5bfaa (6 expanded codes).
--
-- Consumed by int_ltc_lcs_cf_cvd_61 (adds Rule 1) and as a mutual-exclusion set by 62-66.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_61_base') }}
),

qrisk_ge20 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("89e5bfaa-b61e-92e0-8e4d-00d41f703b64") }})
    where result_value >= 20
)

select distinct b.person_id
from base as b
inner join qrisk_ge20 as q
    on b.person_id = q.person_id
