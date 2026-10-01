-- LTC LCS: Hypertension Register - Priority Group 3B (Medium Risk b)
-- Parent population: Hypertension register
-- EMIS source: 'On Hypertension Register- LTC LCS Priority Group 3B (MRb) v3'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/hypertension/
--  on_hypertension_register_ltc_lcs_priority_group_3b_mrb_v3.md)
--
-- Rule chain (exclusion-shaped in EMIS):
-- - Rule 1 (gate): BP monitoring code (vs1 CLINBP_COD or vs2 HOMEAMBBP_COD)
--   in last 12 months. Fail -> exclude.
-- - Rule 2: exclude PG1, PG2, and PG3A v3.
-- - Rules 3-4: exclude patients whose latest reading is controlled at stage 1 -
--   clinic <= 140/90, home/ABPM <= 135/85.
-- - Rule 5: age <= 80 -> include (survived rules 3-4).
-- - Rules 6-7 (age > 80 only): exclude if controlled at the relaxed
--   thresholds - clinic <= 150/90, home/ABPM <= 145/85. Else include.
--
-- Net effect: age <= 80 uses stage 1 thresholds; age > 80 uses the relaxed
-- systolic thresholds (rules 3-4 exclusions are subsets of rules 6-7 for
-- over-80s). No comorbidity required.
--
-- Because EMIS excludes the controlled, gated patients whose readings do not
-- resolve to a paired value are INCLUDED. Controlled checks use the single
-- latest paired BP event (int_blood_pressure_latest, the established rs
-- convention).

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct
        person_id,
        age
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Rule 2: Exclude PG1, PG2, PG3A
higher_pg_exclusions as (
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg2_hr') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg3a_mra') }}
),

-- Rule 1: BP monitoring code in last 12 months
rule_1_bp_code as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3b_mrb_v3_vs1, on_htn_reg_priority_group_3b_mrb_v3_vs2") }})
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

-- Rules 3-4 / 6-7: latest reading controlled at age-banded thresholds
-- Age <= 80: clinic <= 140/90, home/ABPM <= 135/85
-- Age > 80:  clinic <= 150/90, home/ABPM <= 145/85
bp_controlled as (
    select hr.person_id
    from hypertension_register hr
    inner join latest_bp bp on hr.person_id = bp.person_id
    where (
        coalesce(hr.age, 0) <= 80
        and (
            (not bp.is_home_or_abpm and bp.systolic_value <= 140 and bp.diastolic_value <= 90)
            or
            (bp.is_home_or_abpm and bp.systolic_value <= 135 and bp.diastolic_value <= 85)
        )
    )
    or (
        coalesce(hr.age, 0) > 80
        and (
            (not bp.is_home_or_abpm and bp.systolic_value <= 150 and bp.diastolic_value <= 90)
            or
            (bp.is_home_or_abpm and bp.systolic_value <= 145 and bp.diastolic_value <= 85)
        )
    )
),

-- Combine rule results for all hypertension register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_bp_code,
        (ctrl.person_id is not null) as bp_controlled,
        case
            when r1.person_id is null then 'Excluded'
            when ctrl.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hypertension_register hr
    left join higher_pg_exclusions excl on hr.person_id = excl.person_id
    left join rule_1_bp_code r1 on hr.person_id = r1.person_id
    left join bp_controlled ctrl on hr.person_id = ctrl.person_id
    where excl.person_id is null  -- Exclude higher priority groups
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '3B' as priority_group,
    'MRb' as risk_group
from patient_rules
where final_status = 'Included'
