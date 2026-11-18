{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_65_woEX (UCLP Priority Group 3a), pre-Rule-1 population.
-- Keep the latest record across the full 22-code systolic or diastolic pool.
-- Test that same record against the clinic/home code set before its numeric threshold.
-- CHD/stroke/TIA/PAD exclude Review and Ended episodes; CKD/diabetes are dated on or before today.
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-65.md
--
-- Start = int_ltc_lcs_cf_htn_base. Net logic:
--   REQUIRE stage-1 BP:
--     clinic systolic  (latest full pool, code IN vs2) >= 140
--     clinic diastolic (latest full pool, code IN vs4) >= 90
--     home   systolic  (latest full pool, code IN vs5) >= 135
--     home   diastolic (latest full pool, code IN vs6) >= 85
--   EXCLUDE ICB_CF_HTN_61_woEX, 62_woEX and 63_woEX
--   AND an include arm (any of): CHD_COD (vs7) OR STRK_COD (vs8) OR TIA_COD (vs9)
--     OR PAD_COD (vs10) OR CKD_COD (vs11) OR eGFR-MDRD (vs12) latest < 60 OR DM_COD (vs13)
--     OR Black ethnicity (vs14, 78 codes).  NOTE: no BMI arm in PG3a (unlike 63).
-- vs1 [COLLISION] -> pin woEX id 4df8a74f-daed-fda0-5a8d-81d6f775ab83 (22-code combined systolic).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_htn_base') }}
),

htn_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_61_woEX') }}),
htn_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_62_woEX') }}),
htn_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_63_woEX') }}),

clinic_systolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("4df8a74f-daed-fda0-5a8d-81d6f775ab83", "uclp_priority_group_3a_vs2") }})
    where result_value >= 140
),
clinic_diastolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("uclp_priority_group_3a_vs3", "uclp_priority_group_3a_vs4") }})
    where result_value >= 90
),
home_systolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("4df8a74f-daed-fda0-5a8d-81d6f775ab83", "uclp_priority_group_3a_vs5") }})
    where result_value >= 135
),
home_diastolic as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set("uclp_priority_group_3a_vs3", "uclp_priority_group_3a_vs6") }})
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
    select obs.person_id
    from ({{ get_ltc_lcs_observations(
        "uclp_priority_group_3a_vs7,uclp_priority_group_3a_vs8,uclp_priority_group_3a_vs9,uclp_priority_group_3a_vs10"
    ) }}) as obs
    where not exists (
        select 1
        from {{ ref('stg_olids_enriched_concept_map') }} as episode
        where episode.source_concept_id = obs.episodicity_source_concept_id
            and episode.source_system in (
                'EMIS_ObservationEpisode_cs', 'EMIS_Enumeration_cs'
            )
            and upper(episode.source_code) in ('REVIEW', 'ENDED')
    )
    union
    select person_id
    from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs11") }})
    where clinical_effective_date <= current_date()
    union
    select person_id from egfr_low
    union
    select person_id
    from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs13") }})
    where clinical_effective_date <= current_date()
    union
    select person_id
    from ({{ get_ltc_lcs_observations("uclp_priority_group_3a_vs14") }})
)

select distinct b.person_id
from base as b
inner join bp_eligible as bp on b.person_id = bp.person_id
inner join risk_factor as rf on b.person_id = rf.person_id
where b.person_id not in (select person_id from htn_61_woex)
    and b.person_id not in (select person_id from htn_62_woex)
    and b.person_id not in (select person_id from htn_63_woex)
