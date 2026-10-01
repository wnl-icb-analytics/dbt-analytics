{{ config(materialized='table') }}

-- LTC LCS AF case finding indicator AF_62: over 65s missing a pulse check
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/af/icb-cf-af-62.md
-- ICB_CF_AF_62_BASE: currently registered, age >= 65, NOT in ICS_METABOLIC_LTC,
--   NOT CF_NHSHC2Y, NOT on AF register (inside ICS_METABOLIC_LTC), and EXCLUDE everyone
--   already in the ICB_CF_AF_61_base_woEX population (the AF-medication candidates).
-- ICB_CF_AF_62_woEX: include only patients with NO pulse check in the last 3 years
--   (pulse valuesets vs1 10-code [name collides -> pinned id], vs2 2, vs3 8, vs4 28).
-- Rule 1 (final, exclude if matched): AF excluded/confirmed (2-code marker, pinned id)
--   within the last 3 years.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 65
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

af_61_candidates as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_af_61_base_woEX') }}
),

-- Pulse check in the last 3 years (any pulse valueset) -> NOT missing -> exclude
pulse_checked as (
    select person_id
    from ({{ get_ltc_lcs_observations("c73a0da9-a1dd-594d-7f07-fac41d07cb80") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations("af_case_finding_eligible_population_over_65s_missing_pulse_vs2") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations("af_case_finding_eligible_population_over_65s_missing_pulse_vs3") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations("af_case_finding_eligible_population_over_65s_missing_pulse_vs4") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
),

-- Rule 1: AF excluded/confirmed in the last 3 years
af_recorded as (
    select person_id
    from ({{ get_ltc_lcs_observations("5bceb829-7789-d579-1163-85c6908516e6") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from af_61_candidates)
    and b.person_id not in (select person_id from pulse_checked)
    and b.person_id not in (select person_id from af_recorded)
