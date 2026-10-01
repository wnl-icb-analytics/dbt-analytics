{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_64_BASE population (shared helper)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-64.md
--
-- The higher-BMI-band base cohort. Consumed by int_ltc_lcs_cf_dm_64_woEX AND excluded by
-- ICB_CF_DM_65_BASE (so DM_65 only sees the lower BMI band).
--
-- ICB_CF_DM_64_BASE: base (age >= 17, NOT ICS_METABOLIC_LTC, NOT CF_NHSHC2Y), then latest
--   BMI (vs1, 1-code Body mass index) Latest 1 where:
--     numeric value >= 35; OR
--     numeric value >= 32.5 AND patient matches the ethnic-category determinant = vs2 OR vs3.
-- NB: canon's "bame_population" vs2/vs3 are generic EMIS ethnic-category parent concepts
-- (include_children -> all recorded ethnicities, incl. White), so this branch is effectively
-- "BMI>=32.5 AND any recorded ethnicity", not BAME-specific. Implemented per canon; flagged
-- for EMIS/clinical review.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

latest_bmi as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations_latest("obesity_with_latest_bmi_35_325_bame_population_vs1") }})
    where result_value > 0
),

bame as (
    -- canon BAME determinant = vs2 OR vs3 (vs2 pinned by id; see header note on breadth)
    select distinct person_id
    from ({{ get_ltc_lcs_observations("55c83e52-63f7-477c-82c8-9b172815352c, obesity_with_latest_bmi_35_325_bame_population_vs3") }})
)

select distinct b.person_id
from base as b
inner join latest_bmi as m
    on b.person_id = m.person_id
left join bame as e
    on b.person_id = e.person_id
where m.result_value >= 35
    or (m.result_value >= 32.5 and e.person_id is not null)
