{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_62_woEX (UCLP Priority Group 2a) — pre-Rule-1
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-62.md
--
-- Start = int_ltc_lcs_cf_htn_base, EXCLUDE ICB_CF_HTN_61_woEX. Include if ANY arm's LATEST
-- reading meets its threshold:
--   clinic systolic  (vs1 + vs2) latest >= 160
--   clinic diastolic (vs3 + vs4) latest >= 100
--   home   systolic  (vs1 + vs5) latest >= 150
--   home   diastolic (vs3 + vs6) latest >= 95
-- vs1 [COLLISION] -> pin woEX id dfa19d4f-2d74-deab-3a99-ab0ea818a3c5 (22-code clinic-systolic).

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
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("dfa19d4f-2d74-deab-3a99-ab0ea818a3c5, uclp_priority_group_2a_vs2") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 160
),

clinic_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_2a_vs3, uclp_priority_group_2a_vs4") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 100
),

home_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("dfa19d4f-2d74-deab-3a99-ab0ea818a3c5, uclp_priority_group_2a_vs5") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 150
),

home_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_2a_vs3, uclp_priority_group_2a_vs6") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
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
