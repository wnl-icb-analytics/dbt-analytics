{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_63_woEX population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-63.md
--
-- Pre-Rule-1 DM_63 population. Consumed by int_ltc_lcs_cf_dm_63 and subtracted by DM_64-66.
--
-- ICB_CF_DM_63_woEX: base (age >= 17, NOT ICS_METABOLIC_LTC, NOT CF_NHSHC2Y), exclude
--   anyone in ICB_CF_DM_61_woEX OR ICB_CF_DM_62_woEX, then keep patients whose latest HbA1c
--   (3-code IFCC, value > 0, pinned woEX id) is >= 46 and < 48 and dated <= today - 1 year.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

latest_hba1c as (
    select person_id, result_value, clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("160ec384-bb4f-915b-9a8e-66d118eefe0c") }})
    where result_value > 0
),

hba1c_46_47 as (
    select person_id
    from latest_hba1c
    where result_value >= 46
        and result_value < 48
        and clinical_effective_date <= dateadd(year, -1, current_date())
),

dm_61_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_61_woEX') }}
),

dm_62_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_dm_62_woEX') }}
)

select distinct b.person_id
from base as b
inner join hba1c_46_47 as h
    on b.person_id = h.person_id
where b.person_id not in (select person_id from dm_61_woex)
    and b.person_id not in (select person_id from dm_62_woex)
