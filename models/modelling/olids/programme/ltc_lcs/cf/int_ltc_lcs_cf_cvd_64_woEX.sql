{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_64_woEX population (pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-64.md
--
-- CVD_64 shares CVD_63_BASE (age 40-84, not metabolic, not adverse/contra, QRISK latest >=10).
-- CVD_64_woEX = CVD_63_BASE, EXCLUDE CVD_61_woEX, CVD_62_woEX, CVD_63_woEX cohorts,
--   then INCLUDE who do NOT match (i.e. NOT on a statin) any of:
--     - STAT_COD refset medication issue latest > today - 12m (vs1, 845 codes)
--     - STAT_COD refset clinical code latest > today - 12m (vs1, 845 codes)
--     - statin declined latest > today - 5y (vs2, STATINDEC_COD)
-- No QRISK upper bound (QRISK>=10 from BASE; band split handled via prior _woEX exclusion).
--
-- STAT_COD valueset with_a_qrisk2_10_and_not_on_a_statin_vs1 COLLIDES with the 1-code Rule-1
--   marker -> pin woEX id 73a1adb2 (845 STAT_COD codes).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_base') }}
),

cvd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}),
cvd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}),
cvd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}),

-- on a statin / declined (the NOT-match set)
on_statin as (
    -- STAT_COD refset medication issue latest > 12m (vs1, pinned id)
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("73a1adb2-031d-ffa6-1c8a-e0662ddbc9b4") }})
    where order_date > dateadd(month, -12, current_date())
    union
    -- STAT_COD refset clinical code latest > 12m (vs1, pinned id)
    select person_id
    from ({{ get_ltc_lcs_observations_latest("73a1adb2-031d-ffa6-1c8a-e0662ddbc9b4") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    -- statin declined latest > 5y (vs2, STATINDEC_COD)
    select person_id
    from ({{ get_ltc_lcs_observations_latest("with_a_qrisk2_10_and_not_on_a_statin_vs2") }})
    where clinical_effective_date > dateadd(year, -5, current_date())
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from cvd_61_woex)
    and b.person_id not in (select person_id from cvd_62_woex)
    and b.person_id not in (select person_id from cvd_63_woex)
    and b.person_id not in (select person_id from on_statin)
