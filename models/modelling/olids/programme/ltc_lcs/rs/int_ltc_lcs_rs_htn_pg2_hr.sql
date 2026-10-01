-- LTC LCS: Hypertension Register - Priority Group 2 (High Risk)
-- Parent population: Hypertension register
-- EMIS source: 'On Hypertension Register- LTC LCS Priority Group 2 (HR) v3',
-- a wrapper over the library searches 'Priority Group 2a (ICB) v3' and
-- 'Priority Group 2b (ICB) v3'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/hypertension/
--  on_hypertension_register_ltc_lcs_priority_group_2_hr_v3.md and
--  specs/shared/risk_groups/priority_group_2a_icb_v3.md / priority_group_2b_icb_v3.md)
--
-- Rule chain (exclusion-shaped in EMIS):
-- - Gate: BP monitoring code (vs1 CLINBP_COD or vs2 HOMEAMBBP_COD) in last
--   12 months. Fail -> exclude.
-- - Exclude PG1 (HRC) v3.
-- - Include if PG2a OR PG2b:
--   - PG2a: NOT controlled at stage 2 thresholds -
--     clinic <= 160/100, home/ABPM <= 150/95.
--   - PG2b: ethnicity (vs3) AND comorbidity (any of CHD/Stroke/TIA/PAD with
--     episode type not Review or Ended, CKD, eGFR < 60, diabetes, BMI > 35)
--     AND NOT controlled at stage 1 thresholds -
--     clinic <= 140/90, home/ABPM <= 135/85.
--
-- Because EMIS excludes the controlled rather than including the uncontrolled,
-- gated patients whose monitoring codes have no resolvable paired reading are
-- INCLUDED via the PG2a arm. Controlled checks use the single latest paired
-- BP event (int_blood_pressure_latest, the established rs convention).

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Exclude PG1
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
),

-- Gate: BP monitoring code in last 12 months
rule_1_bp_code as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_pg2_hr_v3_vs1, on_htn_reg_pg2_hr_v3_vs2") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Latest paired BP event within 12 months
latest_bp as (
    select
        person_id,
        systolic_value,
        diastolic_value,
        coalesce(is_home_bp_event or is_abpm_bp_event, false) as is_home_or_abpm
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

-- PG2a exclusion: latest reading controlled at stage 2
-- Clinic: SBP <= 160 and DBP <= 100; Home/ABPM: SBP <= 150 and DBP <= 95
pg2a_bp_controlled as (
    select person_id
    from latest_bp
    where (
        (not is_home_or_abpm and systolic_value <= 160 and diastolic_value <= 100)
        or
        (is_home_or_abpm and systolic_value <= 150 and diastolic_value <= 95)
    )
),

-- PG2b exclusion: latest reading controlled at stage 1
-- Clinic: SBP <= 140 and DBP <= 90; Home/ABPM: SBP <= 135 and DBP <= 85
pg2b_bp_controlled as (
    select person_id
    from latest_bp
    where (
        (not is_home_or_abpm and systolic_value <= 140 and diastolic_value <= 90)
        or
        (is_home_or_abpm and systolic_value <= 135 and diastolic_value <= 85)
    )
),

-- PG2b: Ethnicity (vs3)
pg2b_ethnicity as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs3") }})
),

-- PG2b: CHD (episode type not Review or Ended)
pg2b_chd as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs4") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- PG2b: Stroke/TIA (episode type not Review or Ended)
pg2b_stroke_tia as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs5, priority_group_2b_icb_v3_vs6") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- PG2b: PAD (episode type not Review or Ended)
pg2b_pad as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs7") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- PG2b: CKD
pg2b_ckd as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs8") }})
),

-- PG2b: eGFR < 60 (latest value)
pg2b_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("priority_group_2b_icb_v3_vs9") }})
    where result_value < 60
),

-- PG2b: Diabetes
pg2b_diabetes as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("priority_group_2b_icb_v3_vs10") }})
),

-- PG2b: BMI > 35 (latest value)
pg2b_bmi as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("priority_group_2b_icb_v3_vs11") }})
    where result_value > 35
),

pg2b_comorbidities as (
    select person_id from pg2b_chd
    union
    select person_id from pg2b_stroke_tia
    union
    select person_id from pg2b_pad
    union
    select person_id from pg2b_ckd
    union
    select person_id from pg2b_egfr
    union
    select person_id from pg2b_diabetes
    union
    select person_id from pg2b_bmi
),

-- Combine rule results for all hypertension register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_bp_code,
        (a_ctrl.person_id is null) as pg2a_not_controlled,
        (eth.person_id is not null
            and com.person_id is not null
            and b_ctrl.person_id is null) as pg2b_matched,
        case
            when r1.person_id is null then 'Excluded'
            -- PG2a: not controlled at stage 2 (unresolved readings included)
            when a_ctrl.person_id is null then 'Included'
            -- PG2b: ethnicity + comorbidity + not controlled at stage 1
            when eth.person_id is not null
                and com.person_id is not null
                and b_ctrl.person_id is null then 'Included'
            else 'Excluded'
        end as final_status
    from hypertension_register hr
    left join pg1_exclusions pg1 on hr.person_id = pg1.person_id
    left join rule_1_bp_code r1 on hr.person_id = r1.person_id
    left join pg2a_bp_controlled a_ctrl on hr.person_id = a_ctrl.person_id
    left join pg2b_bp_controlled b_ctrl on hr.person_id = b_ctrl.person_id
    left join pg2b_ethnicity eth on hr.person_id = eth.person_id
    left join pg2b_comorbidities com on hr.person_id = com.person_id
    where pg1.person_id is null  -- Exclude PG1 patients
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
