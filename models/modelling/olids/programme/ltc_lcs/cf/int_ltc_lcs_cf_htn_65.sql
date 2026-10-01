{{ config(materialized='table') }}

-- LTC LCS HTN case finding indicator HTN_65: UCLP Priority Group 3a
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-65.md
-- HTN_65 = ICB_CF_HTN_65_woEX, then Rule 1 (final, exclude if matched):
--   exclude patients with a 'Hypertension screening' code (uclp_priority_group_3a_vs1, 1-code
--   headline marker [COLLISION] -> pinned id) recorded in the last 3 years.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_65_woEX') }}
),

screening_recent as (
    select person_id
    from ({{ get_ltc_lcs_observations("590709e2-0364-85a8-2fe6-ebf8af31cddb") }})
    where clinical_effective_date > dateadd(year, -3, current_date())
)

select distinct person_id
from base_woex
where person_id not in (select person_id from screening_recent)
