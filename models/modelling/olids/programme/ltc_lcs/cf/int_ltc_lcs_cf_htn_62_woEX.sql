{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_62_woEX (UCLP Priority Group 2a), pre-Rule-1 population.
-- Keep the latest record across the full 22-code systolic or diastolic pool.
-- Test that same record against the clinic/home code set before its numeric threshold.
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-62.md
--
-- Start = int_ltc_lcs_cf_htn_base, EXCLUDE ICB_CF_HTN_61_woEX. Include if ANY arm's LATEST
-- reading meets its threshold:
--   clinic systolic  (latest full pool, code IN vs2) >= 160
--   clinic diastolic (latest full pool, code IN vs4) >= 100
--   home   systolic  (latest full pool, code IN vs5) >= 150
--   home   diastolic (latest full pool, code IN vs6) >= 95
-- vs1 [COLLISION] -> pin woEX id dfa19d4f-2d74-deab-3a99-ab0ea818a3c5 (22-code combined systolic).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_base') }}
),

htn_61_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_61_woEX') }}
),

clinic_systolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("dfa19d4f-2d74-deab-3a99-ab0ea818a3c5", "uclp_priority_group_2a_vs2") }})
    where result_value >= 160
),

clinic_diastolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("uclp_priority_group_2a_vs3", "uclp_priority_group_2a_vs4") }})
    where result_value >= 100
),

home_systolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("dfa19d4f-2d74-deab-3a99-ab0ea818a3c5", "uclp_priority_group_2a_vs5") }})
    where result_value >= 150
),

home_diastolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("uclp_priority_group_2a_vs3", "uclp_priority_group_2a_vs6") }})
    where result_value >= 95
),

bp_eligible as (
    select person_id from clinic_systolic
    union
    select person_id from clinic_diastolic
    union
    select person_id from home_systolic
    union
    select person_id from home_diastolic
)

select distinct b.person_id
from base as b
inner join bp_eligible as e
    on b.person_id = e.person_id
where b.person_id not in (select person_id from htn_61_woex)
