{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_66_woEX (UCLP Priority Group 3b) — pre-Rule-1
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-66.md
--
-- Start = int_ltc_lcs_cf_htn_base. Net logic:
--   INCLUDE stage-1 BP:
--     clinic systolic  (vs1 + vs2) latest >= 140
--     clinic diastolic (vs3 + vs4) latest >= 90
--     home   systolic  (vs1 + vs5) latest >= 135
--     home   diastolic (vs3 + vs6) latest >= 85
--   EXCLUDE ICB_CF_HTN_61_woEX, 62_woEX, 63_woEX, 65_woEX  (this realises the
--     "no risk factor / not higher priority" intent — NOT re-derived directly per canon)
--   EXCLUDE age>=80 patients whose elderly band is below threshold (BOTH sys AND dia below):
--     clinic: age>=80 AND latest clinic SBP < 150 AND latest clinic DBP < 90
--     home:   age>=80 AND latest home   SBP < 145 AND latest home   DBP < 85
-- vs1 [COLLISION] -> pin woEX id 8c607c29-9273-b754-5056-727bbbc8d5fa (22-code clinic-systolic).

with base as (
    select bp.person_id, pop.age_at_least
    from {{ ref('int_ltc_lcs_cf_htn_base') }} as bp
    inner join {{ ref('int_ltc_lcs_cf_base_population') }} as pop
        on bp.person_id = pop.person_id
),

htn_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_61_woEX') }}),
htn_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_62_woEX') }}),
htn_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_63_woEX') }}),
htn_65_woex as (select person_id from {{ ref('int_ltc_lcs_cf_htn_65_woEX') }}),

-- latest value per BP arm (value carried for both the include test and the 80+ exclusion)
clinic_systolic_latest as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("8c607c29-9273-b754-5056-727bbbc8d5fa, uclp_priority_group_3b_vs2") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
),
clinic_diastolic_latest as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("uclp_priority_group_3b_vs3, uclp_priority_group_3b_vs4") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
),
home_systolic_latest as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("8c607c29-9273-b754-5056-727bbbc8d5fa, uclp_priority_group_3b_vs5") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
),
home_diastolic_latest as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("uclp_priority_group_3b_vs3, uclp_priority_group_3b_vs6") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc, observation_id desc) = 1
),

bp_eligible as (
    select person_id from clinic_systolic_latest where result_value >= 140
    union
    select person_id from clinic_diastolic_latest where result_value >= 90
    union
    select person_id from home_systolic_latest where result_value >= 135
    union
    select person_id from home_diastolic_latest where result_value >= 85
)

select distinct b.person_id
from base as b
inner join bp_eligible as e on b.person_id = e.person_id
left join clinic_systolic_latest as cs on b.person_id = cs.person_id
left join clinic_diastolic_latest as cd on b.person_id = cd.person_id
left join home_systolic_latest as hs on b.person_id = hs.person_id
left join home_diastolic_latest as hd on b.person_id = hd.person_id
where b.person_id not in (select person_id from htn_61_woex)
    and b.person_id not in (select person_id from htn_62_woex)
    and b.person_id not in (select person_id from htn_63_woex)
    and b.person_id not in (select person_id from htn_65_woex)
    -- 80+ elderly-band exclusion: clinic (both sys<150 AND dia<90).
    -- coalesce missing readings above the band so an incomplete BP pair does NOT match the
    -- exclusion (canon "then Latest 1 where value <" requires BOTH readings present and below).
    and not (
        b.age_at_least >= 80
        and coalesce(cs.result_value, 9999) < 150
        and coalesce(cd.result_value, 9999) < 90
    )
    -- 80+ elderly-band exclusion: home (both sys<145 AND dia<85)
    and not (
        b.age_at_least >= 80
        and coalesce(hs.result_value, 9999) < 145
        and coalesce(hd.result_value, 9999) < 85
    )
