{{ config(materialized='table') }}

-- LTC LCS DM case finding: ICB_CF_DM_64_woEX population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/diabetes/icb-cf-dm-64.md
--
-- Pre-Rule-1 DM_64 population. Consumed by int_ltc_lcs_cf_dm_64 and subtracted by DM_65-66.
--
-- ICB_CF_DM_64_woEX: ICB_CF_DM_64_BASE, exclude anyone in ICB_CF_DM_61/62/63_woEX, then
--   keep patients with NO HbA1c (3-code IFCC, date-only, pinned woEX id) in the last 2 years.

with base_64 as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_dm_64_base') }}
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

-- HbA1c recorded in the last 2 years (date-only, no value filter) -> exclude
hba1c_last_2y as (
    select person_id
    from ({{ get_ltc_lcs_observations("5bf8f69d-5b7e-b917-f3bf-4dcda11c8b6f") }})
    where clinical_effective_date >= dateadd(year, -2, current_date())
)

select distinct person_id
from base_64
where person_id not in (select person_id from dm_61_woex)
    and person_id not in (select person_id from dm_62_woex)
    and person_id not in (select person_id from dm_63_woex)
    and person_id not in (select person_id from hba1c_last_2y)
