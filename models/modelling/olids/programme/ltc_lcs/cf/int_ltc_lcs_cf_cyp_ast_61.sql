{{ config(materialized='table') }}

-- CYP_AST_61: possible undiagnosed asthma. One row per included person at build date.
-- Use the eligible parent population, then exclude anyone with a "Respiratory disease screening"
-- marker recorded strictly after three years ago. There is no XML upper date bound.
-- The marker's friendly name collides with another set, so use its warehouse ID.
-- XML authority: ICB_CF_CYPAST_61, R5 updated 27 November 2025.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cyp_ast_61_base_woEX') }}
),

-- Rule 1: Respiratory disease screening recorded in the last 3 years [collision -> headline id 0bda5482]
screening_recorded as (
    select person_id
    from ({{ get_ltc_lcs_observations("0bda5482-083e-41ba-a5eb-da032dd51b16") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from screening_recorded)
