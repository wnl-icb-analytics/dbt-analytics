{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_61_BASE population (shared parent for 61 + 62)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-61.md
--
-- CVD_61_BASE = currently registered, age 40 to <84, INCLUDE who do NOT match any of:
--   - ICS_METABOLIC_LTC (has_metabolic_excluding_condition)
--   - CF_NHSHC2Y (had_nhs_health_check_24m)  [this BASE DOES exclude NHSHC2Y per canon]
--   - atorvastatin SCT_PREP 91-product issue, latest issue date > today - 12m
--     (vs1 people_with_aged_40_84_with_qrisk_recorded_vs1, 91 expanded codes)
--   - STAT_COD refset (12464001000001103) clinical code, latest date > today - 12m
--     (vs2, STAT_COD)
--   - statin adverse reaction (vs3, 52 codes)            -- ever
--   - statin contraindicated / not indicated (vs4, TXSTAT_COD, 3 codes) -- ever
--   - statin declined (vs5, STATINDEC_COD), latest date > today - 5 years
-- (EMIS library items 3de35e4f / ea06414e in canon BASE are not in the export -> not implemented.)
--
-- Statin product valuesets are fully expanded (91 codes) so the medication macro matches
-- directly; no BNF substitution needed.

with base as (
    select person_id, age_at_least
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

age_eligible as (
    select person_id
    from base
    where age_at_least >= 40
        and age_at_least < 84
),

-- atorvastatin SCT_PREP issue, latest > today - 12m (vs1)
on_atorvastatin as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("people_with_aged_40_84_with_qrisk_recorded_vs1") }})
    where order_date > dateadd(month, -12, current_date())
),

-- STAT_COD refset clinical code, latest > today - 12m (vs2)
on_statin_code as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("people_with_aged_40_84_with_qrisk_recorded_vs2") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- statin adverse reaction (vs3) -- ever
statin_adverse as (
    select person_id
    from ({{ get_ltc_lcs_observations("people_with_aged_40_84_with_qrisk_recorded_vs3") }})
),

-- statin contraindicated / not indicated (vs4 TXSTAT_COD) -- ever
statin_contra as (
    select person_id
    from ({{ get_ltc_lcs_observations("people_with_aged_40_84_with_qrisk_recorded_vs4") }})
),

-- statin declined (vs5 STATINDEC_COD), latest > today - 5y
statin_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("people_with_aged_40_84_with_qrisk_recorded_vs5") }})
    where clinical_effective_date > dateadd(year, -5, current_date())
)

select distinct person_id
from age_eligible
where person_id not in (select person_id from on_atorvastatin)
    and person_id not in (select person_id from on_statin_code)
    and person_id not in (select person_id from statin_adverse)
    and person_id not in (select person_id from statin_contra)
    and person_id not in (select person_id from statin_declined)
