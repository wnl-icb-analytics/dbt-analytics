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
--     numeric value >= 32.5 AND ethnicity in vs2 with no vs2 observation in vs3.
-- Interpret Latest 999 + NOTIN as every ethnicity record passing, following
-- the lead's decision based on CKD PG2 EMIS practice-count comparisons.
-- Require an ethnicity record and no ethnicity record outside the BAME and minority categories.
-- vs3 is the non-BAME list with BAME exception children already removed by expansion.

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
    -- Both match paths contribute; any vs2 observation in vs3 excludes the person.
    select eth.person_id
    from ({{ get_ltc_lcs_observations("55c83e52-63f7-477c-82c8-9b172815352c") }}) as eth
    left join ({{ get_ltc_lcs_observations("obesity_with_latest_bmi_35_325_bame_population_vs3") }}) as non_bame
        on non_bame.observation_id = eth.observation_id
    group by eth.person_id
    having count(non_bame.observation_id) = 0
)

select distinct b.person_id
from base as b
inner join latest_bmi as m
    on b.person_id = m.person_id
left join bame as e
    on b.person_id = e.person_id
where m.result_value >= 35
    or (m.result_value >= 32.5 and e.person_id is not null)
