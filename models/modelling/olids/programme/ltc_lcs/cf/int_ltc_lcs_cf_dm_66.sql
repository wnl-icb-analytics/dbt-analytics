{{ config(materialized='table') }}

-- LTC LCS DM case finding indicator DM_66: latest HbA1c 42-45, no HbA1c in the last year
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-66.md
--
-- Lowest-priority DM cohort; no later report subtracts it, so woEX is folded inline.
-- ICB_CF_DM_66_woEX: base (age >= 17, NOT ICS_METABOLIC_LTC, NOT CF_NHSHC2Y), exclude
--   ICB_CF_DM_61/62/63/64/65_woEX, then latest HbA1c (3-code IFCC, value > 0, pinned woEX id)
--   >= 42 and < 46 and dated <= today - 1 year.
-- Rule 1 (final, exclude if matched): "annual review declined" (1-code marker, pinned id)
--   within the last 3 years.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

latest_hba1c as (
    select person_id, result_value, clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("a4dfc336-6ec7-4cec-21fa-c7d3e7842f2d") }})
    where result_value > 0
),

hba1c_42_45 as (
    select person_id
    from latest_hba1c
    where result_value >= 42
        and result_value < 46
        and clinical_effective_date <= dateadd(year, -1, current_date())
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

dm_65_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_65_woEX') }}
),

-- Rule 1: annual-review-declined recorded in the last 3 years
review_declined as (
    select person_id
    from ({{ get_ltc_lcs_observations("343c45e1-2edb-f252-16d1-d6e1955659ea") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct b.person_id
from base as b
inner join hba1c_42_45 as h
    on b.person_id = h.person_id
where b.person_id not in (select person_id from dm_61_woex)
    and b.person_id not in (select person_id from dm_62_woex)
    and b.person_id not in (select person_id from dm_63_woex)
    and b.person_id not in (select person_id from dm_64_woex)
    and b.person_id not in (select person_id from dm_65_woex)
    and b.person_id not in (select person_id from review_declined)
