{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_62_woEX population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-62.md
--
-- Pre-Rule-1 DM_62 population. Consumed by int_ltc_lcs_cf_dm_62 and subtracted by DM_63-66.
--
-- ICB_CF_DM_62_BASE: base (age >= 17, NOT ICS_METABOLIC_LTC, NOT CF_NHSHC2Y) AND on a
--   gestational-diabetes code (patients_on_gestational_dm_vs1, 5 codes).
-- ICB_CF_DM_62_woEX: then exclude anyone in ICB_CF_DM_61_woEX, and keep only patients with
--   NO HbA1c (3-code IFCC, date-only, pinned woEX id) in the last 1 year.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

gestational_dm as (
    select person_id
    from ({{ get_ltc_lcs_observations("patients_on_gestational_dm_vs1") }})
),

dm_61_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_61_woEX') }}
),

-- HbA1c recorded in the last 1 year (date-only, no value filter) -> NOT missing -> exclude
hba1c_last_1y as (
    select person_id
    from ({{ get_ltc_lcs_observations("56fd73ba-981e-cab3-0404-8c26b02afb36") }})
    where clinical_effective_date >= dateadd(year, -1, current_date())
)

select distinct b.person_id
from base as b
inner join gestational_dm as g
    on b.person_id = g.person_id
where b.person_id not in (select person_id from dm_61_woex)
    and b.person_id not in (select person_id from hba1c_last_1y)
