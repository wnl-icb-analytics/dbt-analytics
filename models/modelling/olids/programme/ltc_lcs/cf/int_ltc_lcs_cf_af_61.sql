{{ config(materialized='table') }}

-- AF_61: AF medication candidates without recorded AF. One row per included person at build date.
-- Use the eligible parent population, then exclude anyone with a "AF excluded/confirmed"
-- marker recorded strictly after three years ago. There is no XML upper date bound.
-- The marker's friendly name collides with another set, so use its warehouse ID.
-- EMIS-local markers use the shared equivalence rule.
-- XML authority: ICB_CF_AF_61, R5 updated 27 November 2025.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_af_61_base_woEX') }}
),

-- Rule 1: AF excluded/confirmed recorded in the last 3 years
af_recorded as (
    select person_id
    from ({{ get_ltc_lcs_observations("9d65ae05-99d1-8995-36b4-d64960af37e5") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from af_recorded)
