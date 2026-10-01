{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_62_woEX population (pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-62.md
--
-- CVD_62 shares CVD_61_BASE. CVD_62_woEX = CVD_61_BASE, EXCLUDE the CVD_61_woEX cohort,
--   then INCLUDE who match QRISK refset (999011011000230107) latest numeric value >= 15 and < 20.
-- QRISK woEX valueset name people_at_high_risk_cvd_eligible_population_with_qr15_20_vs1
--   -> id 61b18223 (6 expanded codes, CVDASS2_COD). Distinct name; id pinned for safety.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_61_base') }}
),

cvd_61_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}
),

qrisk_15_20 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("61b18223-5ea9-7e25-e497-f81bde391db3") }})
    where result_value >= 15
        and result_value < 20
)

select distinct b.person_id
from base as b
inner join qrisk_15_20 as q
    on b.person_id = q.person_id
where b.person_id not in (select person_id from cvd_61_woex)
