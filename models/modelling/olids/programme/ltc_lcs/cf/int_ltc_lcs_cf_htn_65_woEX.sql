{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_65_woEX (UCLP Priority Group 3a) — pre-Rule-1
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-65.md
--
-- Start = int_ltc_lcs_cf_htn_base. Net logic:
--   REQUIRE stage-1 BP:
--     clinic systolic  (vs1 + vs2) latest >= 140
--     clinic diastolic (vs3 + vs4) latest >= 90
--     home   systolic  (vs1 + vs5) latest >= 135
--     home   diastolic (vs3 + vs6) latest >= 85
--   EXCLUDE ICB_CF_HTN_61_woEX, 62_woEX and 63_woEX
--   AND an include arm (any of): CHD_COD (vs7) OR STRK_COD (vs8) OR TIA_COD (vs9)
--     OR PAD_COD (vs10) OR CKD_COD (vs11) OR eGFR-MDRD (vs12) latest < 60 OR DM_COD (vs13)
--     OR Black ethnicity (vs14, 78 codes).  NOTE: no BMI arm in PG3a (unlike 63).
-- vs1 [COLLISION] -> pin woEX id 4df8a74f-daed-fda0-5a8d-81d6f775ab83 (22-code clinic-systolic).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_base') }}
),

htn_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_61_woEX') }}),
htn_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_62_woEX') }}),
htn_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_63_woEX') }}),

clinic_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("4df8a74f-daed-fda0-5a8d-81d6f775ab83, uclp_priority_group_3a_vs2") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 140
),
clinic_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs3, uclp_priority_group_3a_vs4") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 90
),
home_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("4df8a74f-daed-fda0-5a8d-81d6f775ab83, uclp_priority_group_3a_vs5") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 135
),
home_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs3, uclp_priority_group_3a_vs6") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 85
),

bp_eligible as (
    select person_id from clinic_systolic
    union
    select person_id from clinic_diastolic
    union
    select person_id from home_systolic
    union
    select person_id from home_diastolic
),

egfr_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("uclp_priority_group_3a_vs12") }})
    where result_value < 60
),
risk_factor as (
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs7") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs8") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs9") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs10") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs11") }})
    union
    select person_id from egfr_low
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs13") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs14") }})
)

select distinct b.person_id
from base as b
inner join bp_eligible as bp on b.person_id = bp.person_id
inner join risk_factor as rf on b.person_id = rf.person_id
where b.person_id not in (select person_id from htn_61_woex)
    and b.person_id not in (select person_id from htn_62_woex)
    and b.person_id not in (select person_id from htn_63_woex)
