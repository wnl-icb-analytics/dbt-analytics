{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_61_woEX population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-61.md
--
-- Pre-Rule-1 DM_61 population. Consumed by int_ltc_lcs_cf_dm_61 (which adds the final
-- Rule 1) and subtracted by every higher DM indicator (62-66) as the priority cohort.
--
-- ICB_CF_DM_61_BASE: currently registered, age >= 17, NOT in ICS_METABOLIC_LTC (the DM
--   register exclusion = "no diagnosis of diabetes"), NOT CF_NHSHC2Y.
-- ICB_CF_DM_61_woEX: then
--   - latest HbA1c (vs3, 3-code IFCC, value > 0) >= 48, AND
--   - exclude diabetes resolved / in-remission markers, "Latest 1" semantics. vs1 (7) and
--     vs2 (4, subset of vs1) are wholly remission/resolved codes, so latest-of-set is always
--     a remission marker -> equivalent to any-time membership of vs1 (pinned woEX id, name
--     collides with the 1-code Rule-1 marker).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

-- Latest HbA1c (3-code IFCC valueset), value > 0
latest_hba1c as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations_latest("latest_hba1c_48_and_no_diagnosis_of_dm_vs3") }})
    where result_value > 0
),

hba1c_ge_48 as (
    select person_id
    from latest_hba1c
    where result_value >= 48
),

-- Diabetes resolved / in-remission exclusion.
-- NB: canon vs1 (b7ce1049) is POLLUTED in the reference expansion with the 3 IFCC HbA1c
-- *measurement* codes (999791000000106 etc.), so it matches ~1.2M HbA1c-tested patients and
-- would wrongly exclude the whole cohort. vs2 (68968843) is the clean 4-code diabetes
-- resolved/remission set (Type I/II remission, diabetes resolved, DM in remission). Canon
-- excludes vs1 (any) OR vs2 (Latest 1); vs1's genuine remission content == vs2, so vs2
-- any-time membership reproduces the intended exclusion without the polluted HbA1c codes.
diabetes_remission as (
    select person_id
    from ({{ get_ltc_lcs_observations("68968843-a4fb-1a67-29d0-bfe7f33b241e") }})
)

select distinct b.person_id
from base as b
inner join hba1c_ge_48 as h
    on b.person_id = h.person_id
where b.person_id not in (select person_id from diabetes_remission)
