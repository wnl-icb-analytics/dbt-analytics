{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_63_woEX (UCLP Priority Group 2b) — pre-Rule-1
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-63.md
--
-- Start = int_ltc_lcs_cf_htn_base. Net logic:
--   REQUIRE Black ethnicity (vs1, 78 codes [COLLISION] -> pin woEX id) AND stage-1 BP:
--     clinic systolic  (vs2 + vs3) latest >= 140
--     clinic diastolic (vs4 + vs5) latest >= 90
--     home   systolic  (vs2 + vs6) latest >= 135
--     home   diastolic (vs4 + vs7) latest >= 85
--   EXCLUDE ICB_CF_HTN_61_woEX and ICB_CF_HTN_62_woEX
--   AND a CVD/metabolic risk-factor include (any of):
--     CHD_COD (vs8) OR STRK_COD (vs9) OR TIA_COD (vs10) OR PAD_COD (vs11) OR CKD_COD (vs12)
--     OR eGFR-MDRD (vs13, single code) latest < 60 OR DM_COD (vs14) OR BMI (vs15) latest > 35

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_base') }}
),

htn_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_61_woEX') }}),
htn_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_62_woEX') }}),

-- Require Black ethnicity (vs1, pinned woEX 78-code id)
black_ethnicity as (
    select person_id
    from ({{ get_ltc_lcs_observations("feda64d2-aba7-a4ce-0365-8e7f0237d461") }})
),

clinic_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs2, uclp_priority_group_2b_vs3") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 140
),
clinic_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs4, uclp_priority_group_2b_vs5") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 90
),
home_systolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs2, uclp_priority_group_2b_vs6") }})
        qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
    )
    where result_value >= 135
),
home_diastolic as (
    select person_id
    from (
        select person_id, clinical_effective_date, result_value
        from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs4, uclp_priority_group_2b_vs7") }})
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

-- Risk-factor include (register-code arms ever; eGFR/BMI Latest 1)
egfr_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("uclp_priority_group_2b_vs13") }})
    where result_value < 60
),
bmi_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("uclp_priority_group_2b_vs15") }})
    where result_value > 35
),
risk_factor as (
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs8") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs9") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs10") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs11") }})
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs12") }})
    union
    select person_id from egfr_low
    union
    select person_id from ({{ get_ltc_lcs_observations("uclp_priority_group_2b_vs14") }})
    union
    select person_id from bmi_high
)

select distinct b.person_id
from base as b
inner join black_ethnicity as eth on b.person_id = eth.person_id
inner join bp_eligible as bp on b.person_id = bp.person_id
inner join risk_factor as rf on b.person_id = rf.person_id
where b.person_id not in (select person_id from htn_61_woex)
    and b.person_id not in (select person_id from htn_62_woex)
