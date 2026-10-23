{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_61_woEX population (shared parent)
-- Filter HbA1c values > 0 before Latest 1, then test thresholds and dates.
-- Equivalent local roots permit mapped matches; other local roots match source codes exactly.
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-61.md
--
-- Pre-Rule-1 DM_61 population. Consumed by int_ltc_lcs_cf_dm_61 (which adds the final
-- Rule 1) and subtracted by every higher DM indicator (62-66) as the priority cohort.
--
-- ICB_CF_DM_61_BASE: currently registered, age >= 17, NOT in ICS_METABOLIC_LTC (the DM
--   register exclusion = "no diagnosis of diabetes"), NOT CF_NHSHC2Y.
-- ICB_CF_DM_61_woEX: then
--   - latest HbA1c (vs3, 3-code IFCC, value > 0) >= 48, AND
--   - exclude only when the latest record across the seven-code remission/HbA1c pool
--     belongs to the four-code remission/resolution set. A later HbA1c defeats an older
--     remission marker. The pinned inclusion ID avoids the headline report's name collision.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

-- Latest HbA1c (3-code IFCC valueset), value > 0
latest_hba1c as (
    select person_id, result_value, clinical_effective_date
    from ({{ get_ltc_lcs_observations("919fb132-083e-bba0-d8b6-d21e9846dcae", include_date_recorded=true) }})
    where result_value > 0
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

hba1c_ge_48 as (
    select person_id
    from latest_hba1c
    where result_value >= 48
),

-- Rank the mixed pool before testing the kept record for remission.
diabetes_remission as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set(
        "b7ce1049-b4c0-1477-ec22-7696854f03ed",
        "68968843-a4fb-1a67-29d0-bfe7f33b241e"
    ) }})
)

select distinct b.person_id
from base as b
inner join hba1c_ge_48 as h
    on b.person_id = h.person_id
where b.person_id not in (select person_id from diabetes_remission)
