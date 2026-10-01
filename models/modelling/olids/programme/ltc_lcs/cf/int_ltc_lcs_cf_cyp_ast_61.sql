{{ config(materialized='table') }}

-- LTC LCS CYP asthma case finding indicator CYP_AST_61: possible undiagnosed asthma
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/asthma_cyp/icb-cf-cypast-61.md
-- ICB_CF_CYPAST_61 = ICB_CF_CYPAST_61_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with a "Respiratory disease screening" code
--   (asthma_casefinding_eligible_patients_vs1, 1-code SNOMED marker, name collides with the
--   469-code woEX med set -> pinned headline id) within the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cyp_ast_61_base_woEX') }}
),

-- Rule 1: Respiratory disease screening recorded in the last 3 years [collision -> headline id 0bda5482]
screening_recorded as (
    select person_id
    from ({{ get_ltc_lcs_observations("0bda5482-083e-41ba-a5eb-da032dd51b16") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from screening_recorded)
