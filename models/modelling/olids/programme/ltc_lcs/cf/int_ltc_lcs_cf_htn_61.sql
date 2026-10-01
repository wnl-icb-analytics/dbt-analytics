{{ config(materialized='table') }}

-- LTC LCS HTN case finding indicator HTN_61: UCLP Priority Group 1 (highest risk)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-61.md
-- HTN_61 = ICB_CF_HTN_61_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with a 'Hypertension screening' code (uclp_pg1_highest_risk_vs1, 1-code
--   headline marker [COLLISION] -> pinned id) recorded in the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_61_woEX') }}
),

screening_recent as (
    select person_id
    from ({{ get_ltc_lcs_observations("166bb252-61af-1b31-8ebc-e651bbbd4f29") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from screening_recent)
