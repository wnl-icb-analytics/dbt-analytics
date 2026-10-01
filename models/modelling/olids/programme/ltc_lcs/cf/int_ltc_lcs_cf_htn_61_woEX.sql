{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_61_woEX (UCLP Priority Group 1, highest risk) — pre-Rule-1
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-61.md
--
-- Start = int_ltc_lcs_cf_htn_base. Include if ANY arm's LATEST reading meets its threshold:
--   clinic systolic  (vs1 + vs2) latest >= 180
--   clinic diastolic (vs3 + vs4) latest >= 120
--   home   systolic  (vs1 + vs5) latest >= 170   (vs5 home/ABPM incl "24 hour")
--   home   diastolic (vs3 + vs6) latest >= 115   (vs6 home/ABPM incl "24 hour")
-- vs1 is a [COLLISION] (1-code screening marker in the headline report) -> pin the woEX id
--   9f24e551-90ff-610d-61e7-ca822a831953 (22-code clinic-systolic set).
-- Latest is taken PER ARM (latest across the arm's valuesets combined), not one overall event.
-- Consumed by int_ltc_lcs_cf_htn_61 (adds Rule 1 screening exclusion) and by the
-- mutual-exclusivity of 62/63/65/66_woEX.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_base') }}
),

-- clinic systolic: vs1 (pinned woEX id) + vs2, latest per person >= 180
clinic_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("9f24e551-90ff-610d-61e7-ca822a831953, uclp_pg1_highest_risk_vs2") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 180
),

-- clinic diastolic: vs3 + vs4, latest per person >= 120
clinic_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_pg1_highest_risk_vs3, uclp_pg1_highest_risk_vs4") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 120
),

-- home systolic: vs1 (pinned woEX id) + vs5, latest per person >= 170
home_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("9f24e551-90ff-610d-61e7-ca822a831953, uclp_pg1_highest_risk_vs5") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 170
),

-- home diastolic: vs3 + vs6, latest per person >= 115
home_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_pg1_highest_risk_vs3, uclp_pg1_highest_risk_vs6") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
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
