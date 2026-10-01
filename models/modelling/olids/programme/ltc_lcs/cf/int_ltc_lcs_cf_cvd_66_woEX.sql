{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_66_woEX population (pre-Rule-1)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-66.md
--
-- CVD_66_BASE = currently registered, age 75 to <84, INCLUDE who do NOT match any of:
--   - ICS_METABOLIC_LTC (has_metabolic_excluding_condition)
--   - CF_NHSHC2Y (had_nhs_health_check_24m)
--   - STAT_COD refset medication issue latest > today - 12m (vs1, 845 codes)
--   - STAT_COD refset clinical code latest > today - 12m (vs1, 845 codes)
--   - statin adverse reaction (vs2, 52 codes) -- ever
--   - statin contraindicated / not indicated (vs3, TXSTAT_COD, 3 codes) -- ever
--   - statin declined latest > today - 5y (vs4, STATINDEC_COD)
--   (library items 3de35e4f / ea06414e not in export -> not implemented)
-- CVD_66_woEX = CVD_66_BASE,
--   EXCLUDE CVD_61_woEX..CVD_65_woEX cohorts,
--   then INCLUDE who do NOT match QRISK refset (999011011000230107) Date within the last 5 years.
--
-- QRISK no-record valueset aged_75_84..._vs1 -> id a1c6f038 (6 QRISK refset codes, CVDASS2_COD).

with base as (
    select person_id, age_at_least
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

age_eligible as (
    select person_id
    from base
    where age_at_least >= 75
        and age_at_least < 84
),

cvd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}),
cvd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}),
cvd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}),
cvd_64_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_64_woEX') }}),
cvd_65_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_65_woEX') }}),

-- BASE statin/decline exclusions (the do-NOT-match set)
on_statin as (
    -- STAT_COD refset medication issue latest > 12m (vs1)
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("people_with_aged_70_84_with_qrisk_recorded_vs1") }})
    where order_date > dateadd(month, -12, current_date())
    union
    -- STAT_COD refset clinical code latest > 12m (vs1)
    select person_id
    from ({{ get_ltc_lcs_observations_latest("people_with_aged_70_84_with_qrisk_recorded_vs1") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    -- statin adverse reaction (vs2) -- ever
    select person_id
    from ({{ get_ltc_lcs_observations("people_with_aged_70_84_with_qrisk_recorded_vs2") }})
    union
    -- statin contraindicated / not indicated (vs3 TXSTAT_COD) -- ever
    select person_id
    from ({{ get_ltc_lcs_observations("people_with_aged_70_84_with_qrisk_recorded_vs3") }})
    union
    -- statin declined latest > 5y (vs4 STATINDEC_COD)
    select person_id
    from ({{ get_ltc_lcs_observations_latest("people_with_aged_70_84_with_qrisk_recorded_vs4") }})
    where clinical_effective_date > dateadd(year, -5, current_date())
),

-- QRISK refset recorded in the last 5 years -> has QRISK -> exclude
qrisk_recorded_5y as (
    select person_id
    from ({{ get_ltc_lcs_observations("a1c6f038-fa5a-8265-93aa-1a1eecf71007") }})
    where clinical_effective_date > dateadd(year, -5, current_date())
)

select distinct person_id
from age_eligible
where person_id not in (select person_id from on_statin)
    and person_id not in (select person_id from cvd_61_woex)
    and person_id not in (select person_id from cvd_62_woex)
    and person_id not in (select person_id from cvd_63_woex)
    and person_id not in (select person_id from cvd_64_woex)
    and person_id not in (select person_id from cvd_65_woex)
    and person_id not in (select person_id from qrisk_recorded_5y)
