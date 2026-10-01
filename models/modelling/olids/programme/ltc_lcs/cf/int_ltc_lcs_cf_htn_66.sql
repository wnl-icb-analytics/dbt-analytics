{{ config(materialized='table') }}

-- LTC LCS HTN case finding indicator HTN_66: UCLP Priority Group 3b
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-66.md
-- HTN_66 = ICB_CF_HTN_66_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with a 'Hypertension screening' code (uclp_priority_group_3b_vs1, 1-code
--   headline marker [COLLISION] -> pinned id) recorded in the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_66_woEX') }}
),

screening_recent as (
    select person_id
    from ({{ get_ltc_lcs_observations("a1a449b9-c2c3-792f-b08d-0142c3de3744") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from screening_recent)
