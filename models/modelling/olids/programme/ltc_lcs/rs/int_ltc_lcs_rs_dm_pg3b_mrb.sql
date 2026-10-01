-- LTC LCS: Diabetes Register - Priority Group 3B (Moderate Risk B)
-- Parent population: Diabetes register, excluding PG1 (HRC), PG2 (HR), and PG3A (MRa)
--
-- Logic:
-- - Rule 1: Exclude PG1, PG2, or PG3A
-- - Rule 2: HbA1c 58-75 (include directly)
-- - Rule 3: HbA1c 48-58 (cascade - go to next rule if passed, exclude if failed)
-- - Rule 4: Erectile dysfunction (vs2) OR diabetic retinopathy (vs3) OR BMI >35 (vs4)
--           OR diabetic neuropathy (vs5) OR diabetic foot moderate/high risk (vs6)
--           OR heart failure first episode (vs7) OR GLP-1/insulin in last 6 months (vs9, BNF 060101)
--           OR claudication/atherosclerosis (vs10) (include)
--   Note: vs8 (Insulins) has no SNOMED mapping, using BNF 060101 fallback
-- - Rule 5: eGFR 45-49 (vs11) (include)
-- - Rule 6: ACR 3-30 (vs12) (include)
-- - Rule 7: ABPM BP ≥140/90 (include, if failed → exclude)

with
-- Parent population: Patients on diabetes register
diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

-- Rule 1: Exclude patients already in PG1, PG2, or PG3A
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }}
),

pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg2_hr') }}
),

pg3a_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg3a_mra') }}
),

-- Rule 2: HbA1c 58-75 (direct inclusion)
-- vs1 = HbA1c codes
rule_2_hba1c_58_75 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3b_mrb_vs1") }})
    where result_value > 58 and result_value <= 75
),

-- Rule 3: HbA1c 48-58 (cascade prerequisite for rules 4-7)
-- vs1 = HbA1c codes (same valueset as Rule 2, different threshold)
rule_3_hba1c_48_58 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3b_mrb_vs1") }})
    where result_value > 48 and result_value <= 58
),

-- Rule 4 components (any one qualifies if Rule 3 passed):

-- vs2 = Erectile dysfunction
rule_4_erectile_dysfunction as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3b_mrb_vs2") }})
),

-- vs3 = Diabetic retinopathy
rule_4_diabetic_retinopathy as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3b_mrb_vs3") }})
),

-- vs4 = BMI > 35
rule_4_bmi_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3b_mrb_vs4") }})
    where result_value > 35
),

-- vs5 = Diabetic neuropathy
rule_4_diabetic_neuropathy as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3b_mrb_vs5") }})
),

-- vs6 = Diabetic foot at moderate/high risk
rule_4_diabetic_foot_risk as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3b_mrb_vs6") }})
),

-- vs7 = Heart failure (episode type not Review or Ended)
rule_4_heart_failure as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3b_mrb_vs7") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- GLP-1 medications (vs9) and Insulins in last 6 months
-- Note: vs8 (Insulins drug group) has no SNOMED mapping from terminology server,
-- using BNF 060101 as fallback instead
rule_4_glp1_medications as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_dm_reg_priority_group_3b_mrb_vs9") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_medication_orders(bnf_code='060101') }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),

-- vs10 = Claudication/atherosclerosis
rule_4_claudication as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3b_mrb_vs10") }})
),

-- Combine all Rule 4 components
rule_4_combined as (
    select person_id from rule_4_erectile_dysfunction
    union
    select person_id from rule_4_diabetic_retinopathy
    union
    select person_id from rule_4_bmi_high
    union
    select person_id from rule_4_diabetic_neuropathy
    union
    select person_id from rule_4_diabetic_foot_risk
    union
    select person_id from rule_4_heart_failure
    union
    select person_id from rule_4_glp1_medications
    union
    select person_id from rule_4_claudication
),

-- Rule 5: eGFR 45-49 (latest value)
-- vs11 = eGFR codes
rule_5_egfr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3b_mrb_vs11") }})
    where result_value >= 45 and result_value < 49
),

-- Rule 6: ACR 3-30 (latest value)
-- vs12 = ACR codes
rule_6_acr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3b_mrb_vs12") }})
    where result_value >= 3 and result_value <= 30
),

-- Rule 7: Latest BP ≥140 systolic OR ≥90 diastolic (any BP type)
-- EMIS uses BP_COD (all BP) with linked systolic/diastolic pairing on DATE.
-- Unlike HTN which separates clinic vs home/ABPM thresholds, DM applies
-- a single 140/90 threshold regardless of BP type.
rule_7_bp_high as (
    select person_id
    from {{ ref('int_blood_pressure_all') }}
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
      and (systolic_value >= 140 or diastolic_value >= 90)
),

-- Combine rule results
patient_rules as (
    select
        dr.person_id,
        (r2.person_id is not null) as rule_2_hba1c_58_75,
        (r3.person_id is not null) as rule_3_hba1c_48_58,
        (r4.person_id is not null) as rule_4_complications,
        (r5.person_id is not null) as rule_5_egfr_range,
        (r6.person_id is not null) as rule_6_acr_range,
        (r7.person_id is not null) as rule_7_bp_high,
        case
            -- Rule 2: HbA1c 58-75 is direct inclusion
            when r2.person_id is not null then 'Included'
            -- Rule 3 cascade: Must have HbA1c 48-58 to proceed to rules 4-7
            when r3.person_id is null then 'Excluded'
            -- Rules 4-7: Any one qualifies if Rule 3 passed
            when r4.person_id is not null then 'Included'
            when r5.person_id is not null then 'Included'
            when r6.person_id is not null then 'Included'
            when r7.person_id is not null then 'Included'
            else 'Excluded'  -- Rule 7 failed = exclude from final result
        end as final_status
    from diabetes_register dr
    left join pg1_exclusions pg1 on dr.person_id = pg1.person_id
    left join pg2_exclusions pg2 on dr.person_id = pg2.person_id
    left join pg3a_exclusions pg3a on dr.person_id = pg3a.person_id
    left join rule_2_hba1c_58_75 r2 on dr.person_id = r2.person_id
    left join rule_3_hba1c_48_58 r3 on dr.person_id = r3.person_id
    left join rule_4_combined r4 on dr.person_id = r4.person_id
    left join rule_5_egfr_range r5 on dr.person_id = r5.person_id
    left join rule_6_acr_range r6 on dr.person_id = r6.person_id
    left join rule_7_bp_high r7 on dr.person_id = r7.person_id
    where pg1.person_id is null
      and pg2.person_id is null
      and pg3a.person_id is null
)

select
    person_id,
    final_status,
    'Diabetes' as condition,
    '3b' as priority_group,
    'MRb' as risk_group
from patient_rules
where final_status = 'Included'
