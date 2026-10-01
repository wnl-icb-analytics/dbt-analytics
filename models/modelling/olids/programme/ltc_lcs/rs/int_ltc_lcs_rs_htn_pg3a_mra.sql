-- LTC LCS: Hypertension Register - Priority Group 3A (Medium Risk a)
-- Parent population: Hypertension register
-- EMIS source: 'On Hypertension Register- LTC LCS Priority Group 3A (MRa) v3'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/hypertension/
--  on_hypertension_register_ltc_lcs_priority_group_3a_mra_v3.md)
--
-- Rule chain (exclusion-shaped in EMIS):
-- - Rule 1 (gate): BP monitoring code (vs1 CLINBP_COD or vs2 HOMEAMBBP_COD)
--   in last 12 months. Fail -> exclude.
-- - Rule 2: exclude PG1 (HRC) v3 and PG2 (HR) v3.
-- - Rule 3 (must match): any of - CHD (vs3), Stroke/TIA (vs4/vs5), PAD (vs6)
--   with episode type not Review or Ended; CKD (vs7); latest eGFR < 60 (vs8);
--   diabetes (vs9); ethnicity (vs10).
-- - Rules 4-5: exclude patients whose latest reading is controlled at stage 1 -
--   clinic <= 140/90, home/ABPM <= 135/85. Everyone else -> include.
--
-- Because EMIS excludes the controlled, gated patients with a qualifying
-- comorbidity whose readings do not resolve to a paired value are INCLUDED.
-- Controlled checks use the single latest paired BP event
-- (int_blood_pressure_latest, the established rs convention).

with
-- Parent population: Patients currently on hypertension register
hypertension_register as (
    select distinct person_id
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

-- Rule 2: Exclude PG1 and PG2
higher_pg_exclusions as (
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
    union
    select person_id from {{ ref('int_ltc_lcs_rs_htn_pg2_hr') }}
),

-- Rule 1: BP monitoring code in last 12 months
rule_1_bp_code as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs1, on_htn_reg_priority_group_3a_mra_v3_vs2") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Rule 3: CHD (episode type not Review or Ended)
rule_3_chd as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs3") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- Rule 3: Stroke/TIA (episode type not Review or Ended)
rule_3_stroke_tia as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs4, on_htn_reg_priority_group_3a_mra_v3_vs5") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- Rule 3: PAD (episode type not Review or Ended)
rule_3_pad as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs6") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- Rule 3: CKD
rule_3_ckd as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs7") }})
),

-- Rule 3: eGFR < 60 (latest value)
rule_3_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_htn_reg_priority_group_3a_mra_v3_vs8") }})
    where result_value < 60
),

-- Rule 3: Diabetes
rule_3_diabetes as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs9") }})
),

-- Rule 3: Ethnicity (vs10)
rule_3_ethnicity as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_htn_reg_priority_group_3a_mra_v3_vs10") }})
),

rule_3_comorbidities as (
    select person_id from rule_3_chd
    union
    select person_id from rule_3_stroke_tia
    union
    select person_id from rule_3_pad
    union
    select person_id from rule_3_ckd
    union
    select person_id from rule_3_egfr
    union
    select person_id from rule_3_diabetes
    union
    select person_id from rule_3_ethnicity
),

-- Rules 4-5: latest paired BP event is controlled at stage 1
-- Clinic: SBP <= 140 and DBP <= 90; Home/ABPM: SBP <= 135 and DBP <= 85
rules_4_5_bp_controlled as (
    select person_id
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and (
          (not coalesce(is_home_bp_event or is_abpm_bp_event, false)
           and systolic_value <= 140 and diastolic_value <= 90)
          or
          (coalesce(is_home_bp_event or is_abpm_bp_event, false)
           and systolic_value <= 135 and diastolic_value <= 85)
      )
),

-- Combine rule results for all hypertension register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_bp_code,
        (com.person_id is not null) as rule_3_comorbidity,
        (bpc.person_id is not null) as rules_4_5_bp_controlled,
        case
            when r1.person_id is null then 'Excluded'
            when com.person_id is null then 'Excluded'
            when bpc.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hypertension_register hr
    left join higher_pg_exclusions excl on hr.person_id = excl.person_id
    left join rule_1_bp_code r1 on hr.person_id = r1.person_id
    left join rule_3_comorbidities com on hr.person_id = com.person_id
    left join rules_4_5_bp_controlled bpc on hr.person_id = bpc.person_id
    where excl.person_id is null  -- Exclude PG1 and PG2
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Hypertension' as condition,
    '3A' as priority_group,
    'MRa' as risk_group
from patient_rules
where final_status = 'Included'
