{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_61_woEX (UCLP Priority Group 1, highest risk). Pre-Rule-1
-- Keep the latest record across the full 22-code systolic or diastolic pool.
-- Test that same record against the clinic/home code set before its numeric threshold.
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-61.md
--
-- Start = int_ltc_lcs_cf_htn_base. Include if ANY arm's LATEST reading meets its threshold:
--   clinic systolic  (latest full pool, code IN vs2) >= 180
--   clinic diastolic (latest full pool, code IN vs4) >= 120
--   home   systolic  (latest full pool, code IN vs5) >= 170   (vs5 home/ABPM incl "24 hour")
--   home   diastolic (latest full pool, code IN vs6) >= 115   (vs6 home/ABPM incl "24 hour")
-- vs1 is a [COLLISION] (1-code screening marker in the headline report) -> pin the woEX id
--   9f24e551-90ff-610d-61e7-ca822a831953 (22-code combined systolic set).
-- Consumed by int_ltc_lcs_cf_htn_61 (adds Rule 1 screening exclusion) and by the
-- mutual-exclusivity of 62/63/65/66_woEX.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_base') }}
),

clinic_systolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("9f24e551-90ff-610d-61e7-ca822a831953", "uclp_pg1_highest_risk_vs2") }})
    where result_value >= 180
),

clinic_diastolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("uclp_pg1_highest_risk_vs3", "uclp_pg1_highest_risk_vs4") }})
    where result_value >= 120
),

home_systolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("9f24e551-90ff-610d-61e7-ca822a831953", "uclp_pg1_highest_risk_vs5") }})
    where result_value >= 170
),

home_diastolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("uclp_pg1_highest_risk_vs3", "uclp_pg1_highest_risk_vs6") }})
    where result_value >= 115
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
