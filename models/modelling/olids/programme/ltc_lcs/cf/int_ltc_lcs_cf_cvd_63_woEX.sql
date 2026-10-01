{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_63_woEX population (pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-63.md
--
-- CVD_63_woEX = CVD_63_BASE (age 40-84, not metabolic, not adverse/contra, QRISK latest >=10),
--   then REQUIRE non-HDL cholesterol latest numeric value > 2.5,
--   EXCLUDE CVD_61_woEX and CVD_62_woEX cohorts,
--   then INCLUDE who match (on a statin) any of:
--     - atorvastatin SCT_PREP issue latest > today - 12m (vs2, 91 codes)
--     - STAT_COD refset clinical code latest > today - 12m (vs3, 845 codes)
--     - STAT_COD refset medication issue latest > today - 12m (vs3, 845 codes)
--
-- non-HDL valueset qrisk2_10_on_a_statin_and_a_non_hdl_25_vs1 COLLIDES with the 1-code Rule-1
--   marker -> pin woEX id 23f72268 (4 non-HDL codes).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_base') }}
),

cvd_61_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}
),

cvd_62_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}
),

-- non-HDL cholesterol latest > 2.5 (vs1, pinned id)
non_hdl_gt25 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("23f72268-d2d1-6fb0-9fe5-19eeb497790e") }})
    where result_value > 2.5
),

-- on a statin (any branch)
on_statin as (
    -- atorvastatin SCT_PREP issue latest > 12m (vs2)
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("qrisk2_10_on_a_statin_and_a_non_hdl_25_vs2") }})
    where order_date > dateadd(month, -12, current_date())
    union
    -- STAT_COD refset clinical code latest > 12m (vs3)
    select person_id
    from ({{ get_ltc_lcs_observations_latest("qrisk2_10_on_a_statin_and_a_non_hdl_25_vs3") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    -- STAT_COD refset medication issue latest > 12m (vs3)
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("qrisk2_10_on_a_statin_and_a_non_hdl_25_vs3") }})
    where order_date > dateadd(month, -12, current_date())
)

select distinct b.person_id
from base as b
inner join non_hdl_gt25 as n
    on b.person_id = n.person_id
inner join on_statin as s
    on b.person_id = s.person_id
where b.person_id not in (select person_id from cvd_61_woex)
    and b.person_id not in (select person_id from cvd_62_woex)
