-- LTC LCS: Hypertension Register - Priority Group 1 (High Risk & Complex)
-- Parent population: Hypertension register
-- EMIS source: 'On Hypertension Register- LTC LCS Priority Group 1 (HRC) v3'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/hypertension/on_hypertension_register_ltc_lcs_priority_group_1_hrc_v3.md)
--
-- Rule chain (exclusion-shaped in EMIS):
-- - Rule 1 (gate): BP monitoring code (vs1 CLINBP_COD or vs2 HOMEAMBBP_COD)
--   in last 12 months. Fail -> exclude.
-- - Rules 2-3: exclude patients whose latest reading is controlled -
--   clinic <= 180/120, home/ABPM <= 170/115. Everyone else -> include.
--
-- Because EMIS excludes the controlled rather than including the severe,
-- patients with a BP monitoring code whose same-date reading chains do not
-- resolve are INCLUDED (data-quality catch). This port mirrors that: the
-- gate comes from the monitoring-code valuesets and the controlled check
-- from the single latest paired BP event (int_blood_pressure_latest, the
-- established rs convention).

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Rule 1: BP monitoring code in last 12 months
rule_1_bp_code as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_pg1_hrc_v3_vs1, on_htn_reg_pg1_hrc_v3_vs2") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Rules 2-3: latest paired BP event is controlled
-- Clinic: SBP <= 180 and DBP <= 120; Home/ABPM: SBP <= 170 and DBP <= 115
rules_2_3_bp_controlled as (
    select person_id
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and (
          (not coalesce(is_home_bp_event or is_abpm_bp_event, false)
           and systolic_value <= 180 and diastolic_value <= 120)
          or
          (coalesce(is_home_bp_event or is_abpm_bp_event, false)
           and systolic_value <= 170 and diastolic_value <= 115)
      )
),

-- Combine rule results for all hypertension register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_bp_code,
        (bpc.person_id is not null) as rules_2_3_bp_controlled,
        case
            when r1.person_id is null then 'Excluded'
            when bpc.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hypertension_register hr
    left join rule_1_bp_code r1 on hr.person_id = r1.person_id
    left join rules_2_3_bp_controlled bpc on hr.person_id = bpc.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
