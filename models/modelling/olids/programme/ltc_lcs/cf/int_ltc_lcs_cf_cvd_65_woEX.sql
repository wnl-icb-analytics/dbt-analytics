{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_65_woEX population (pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-65.md
--
-- CVD_65 shares CVD_63_BASE (age 40-84, not metabolic, not adverse/contra, QRISK latest >=10).
-- CVD_65_woEX =
--   REQUIRE QRISK refset (999011011000230107) latest numeric value >= 10 (vs2, CVDASS2_COD),
--   EXCLUDE CVD_61_woEX, CVD_62_woEX, CVD_63_woEX, CVD_64_woEX cohorts,
--   EXCLUDE lipid-lowering adverse-reaction set (vs1, 19 codes),
--   then INCLUDE who match Medication Courses with Atorvastatin/Simvastatin/Fluvastatin +2
--     (vs3, 5 SCT Const ingredients -> 577 expanded statin products).
--
-- Collisions:
--   vs1 with_qrisk210_amd_not_on_a_high_intensity_statin_vs1 -> pin woEX id ed40f2e7 (19 AR codes)
--   vs2 -> pin id fdead4de (6 QRISK refset codes)  [distinct purpose copy of CVDASS2]

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_base') }}
),

cvd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}),
cvd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}),
cvd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}),
cvd_64_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_64_woEX') }}),

-- QRISK refset latest >= 10 (vs2, pinned id)
qrisk_ge10 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("fdead4de-c567-b1c4-15af-7e6a88bfb587") }})
    where result_value >= 10
),

-- lipid-lowering adverse-reaction exclusion (vs1, pinned id, 19 codes) -- ever
lipid_adverse as (
    select person_id
    from ({{ get_ltc_lcs_observations("ed40f2e7-f9f3-1a17-104a-42af018aaf32") }})
),

-- on a statin course (vs3 5-ingredient statin list, 577 expanded products)
on_statin_course as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("with_qrisk210_amd_not_on_a_high_intensity_statin_vs3") }})
)

select distinct b.person_id
from base as b
inner join qrisk_ge10 as q
    on b.person_id = q.person_id
inner join on_statin_course as s
    on b.person_id = s.person_id
where b.person_id not in (select person_id from cvd_61_woex)
    and b.person_id not in (select person_id from cvd_62_woex)
    and b.person_id not in (select person_id from cvd_63_woex)
    and b.person_id not in (select person_id from cvd_64_woex)
    and b.person_id not in (select person_id from lipid_adverse)
