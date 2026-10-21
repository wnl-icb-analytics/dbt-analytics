{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_64_BASE population (shared helper)
-- Local list roots match exactly; local descriptions of public concepts remain eligible.
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
-- Require ethnicity evidence; none of the latest 999 records may fail the category test.
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
    from ({{ get_ltc_lcs_observations_latest("631c11e6-1a77-2cd0-0dd3-3ef61868842c", emis_local_exact=true) }})
    where result_value > 0
),

bame as (
    -- Every record in the latest 999 ethnicity observations must pass the non-BAME test.
    select eth.person_id
    from (
        select *
        from ({{ get_ltc_lcs_observations("55c83e52-63f7-477c-82c8-9b172815352c", emis_local_exact=true) }})
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) <= 999
    ) as eth
    left join ({{ get_ltc_lcs_observations("f0c4af8c-f818-da47-ba29-600c2f13e09f", emis_local_exact=true) }}) as non_bame
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
