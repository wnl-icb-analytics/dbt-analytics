{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_65_woEX population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-65.md
--
-- Pre-Rule-1 DM_65 population. Consumed by int_ltc_lcs_cf_dm_65 and subtracted by DM_66.
--
-- ICB_CF_DM_65_BASE: base (age >= 17, NOT ICS_METABOLIC_LTC, NOT CF_NHSHC2Y), exclude
--   ICB_CF_DM_64_BASE (higher BMI band), then latest BMI (vs1) Latest 1 where:
--     numeric value >= 30 and < 35; OR
--     numeric value >= 27.5 and < 32.5 AND BAME (vs3 17-code list, any-time membership).
-- ICB_CF_DM_65_woEX: then exclude ICB_CF_DM_61/62/63/64_woEX, and keep patients with NO
--   HbA1c (3-code IFCC, date-only, pinned woEX id) in the last 2 years.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

dm_64_base as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_64_base') }}
),

latest_bmi as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations_latest("obesity_with_latest_bmi_30_35_275_325_bame_population_vs1") }})
    where result_value > 0
),

bame as (
    -- canon BAME determinant = vs2 OR vs3 (generic EMIS ethnic-category sets, include_children
    -- -> all recorded ethnicities; see DM_64 header note -- flagged for clinical review)
    select distinct person_id
    from ({{ get_ltc_lcs_observations("2e9a5dbb-a1f2-19ff-d657-bf70de2d2c43, obesity_with_latest_bmi_30_35_275_325_bame_population_vs3") }})
),

base_65 as (
    select distinct b.person_id
    from base as b
    inner join latest_bmi as m
        on b.person_id = m.person_id
    left join bame as e
        on b.person_id = e.person_id
    where b.person_id not in (select person_id from dm_64_base)
        and (
            (m.result_value >= 30 and m.result_value < 35)
            or (m.result_value >= 27.5 and m.result_value < 32.5 and e.person_id is not null)
        )
),

dm_61_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_61_woEX') }}
),

dm_62_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_62_woEX') }}
),

dm_63_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_63_woEX') }}
),

dm_64_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_64_woEX') }}
),

hba1c_last_2y as (
    select person_id
    from ({{ get_ltc_lcs_observations("dd384e6e-92e2-04b5-f1f9-17c119852411") }})
    where clinical_effective_date >= dateadd(year, -2, current_date())
)

select distinct person_id
from base_65
where person_id not in (select person_id from dm_61_woex)
    and person_id not in (select person_id from dm_62_woex)
    and person_id not in (select person_id from dm_63_woex)
    and person_id not in (select person_id from dm_64_woex)
    and person_id not in (select person_id from hba1c_last_2y)
