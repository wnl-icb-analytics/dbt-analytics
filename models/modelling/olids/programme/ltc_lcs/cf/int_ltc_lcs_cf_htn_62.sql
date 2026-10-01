{{ config(materialized='table') }}

-- LTC LCS HTN case finding indicator HTN_62: UCLP Priority Group 2a
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-62.md
-- HTN_62 = ICB_CF_HTN_62_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with a 'Hypertension screening' code (uclp_priority_group_2a_vs1, 1-code
--   headline marker [COLLISION] -> pinned id) recorded in the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_62_woEX') }}
),

screening_recent as (
    select person_id
    from ({{ get_ltc_lcs_observations("80f7370c-2495-0ff5-2c56-92fe5fd37f42") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from screening_recent)
