{{ config(materialized='table') }}

-- LTC LCS AF case finding indicator AF_61: on AF medication, no recorded AF
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/af/icb-cf-af-61.md
-- ICB_CF_AF_61 = ICB_CF_AF_61_base_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with "Atrial fibrillation excluded / confirmed" (2-code marker,
--   name collides with the 223-code woEX set -> pinned id) within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_af_61_base_woEX') }}
),

-- Rule 1: AF excluded/confirmed recorded in the last 3 years
af_recorded as (
    select person_id
    from ({{ get_ltc_lcs_observations("9d65ae05-99d1-8995-36b4-d64960af37e5") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from af_recorded)
