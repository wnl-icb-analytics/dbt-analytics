-- LTC LCS: Diabetes Register - Priority Group 3A (Moderate Risk A)
-- Parent population: Diabetes register, excluding PG1 (HRC) and PG2 (HR)
--
-- Logic:
-- - Rule 1: Exclude PG1 or PG2
-- - Rule 2: HbA1c 58-75 (cascade - go to next rule if passed, exclude if failed)
-- - Rule 3: MI (vs2) or cerebral thrombosis (vs3) or TIA (vs4) in last 12 months (include)
-- - Rule 4: eGFR 30-44 (vs5) (include)
-- - Rule 5: ACR > 30 (vs6) (include)
-- - Rule 6: BP ≥140 systolic OR ≥90 diastolic (include, if failed → exclude)
--
-- BP uses the shared LTC LCS selector with the diabetes XML lists and 140/90 thresholds.

with
-- Parent population: Patients on diabetes register
diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

-- Rule 1: Exclude patients already in PG1 (HRC) or PG2 (HR)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }}
),

pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg2_hr') }}
),

-- Rule 2: HbA1c 58-75 (cascade prerequisite)
-- vs1 = HbA1c codes
rule_2_hba1c_prerequisite as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3a_mra_vs1") }})
    where result_value > 58 and result_value <= 75
),

-- Rule 3: MI (vs2) or cerebral thrombosis (vs3) or TIA (vs4) in last 12 months
rule_3_mi as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3a_mra_vs2") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

rule_3_cerebral_thrombosis as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3a_mra_vs3") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

rule_3_tia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_priority_group_3a_mra_vs4") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

rule_3_combined as (
    select person_id from rule_3_mi
    union
    select person_id from rule_3_cerebral_thrombosis
    union
    select person_id from rule_3_tia
),

-- Rule 4: eGFR 30-44 (latest value)
-- vs5 = eGFR codes
rule_4_egfr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3a_mra_vs5") }})
    where result_value >= 30 and result_value < 44
),

-- Rule 5: ACR > 30 (latest value)
-- vs6 = ACR codes
rule_5_acr_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3a_mra_vs6") }})
    where result_value > 30
),

-- Diabetes uses its own XML component pools and the same-date BP_COD link.
rule_6_bp_high as (
    select person_id
    from {{ ref('int_ltc_lcs_bp_latest_by_setting') }}
    where diabetes_systolic_value >= 140 or diabetes_diastolic_value >= 90
),

-- Combine rule results
-- Rule 2 is a cascade prerequisite: must have HbA1c 58-75 to proceed to rules 3-6
patient_rules as (
    select
        dr.person_id,
        (r2.person_id is not null) as rule_2_hba1c_prerequisite,
        (r3.person_id is not null) as rule_3_mi_stroke_tia,
        (r4.person_id is not null) as rule_4_egfr_range,
        (r5.person_id is not null) as rule_5_acr_high,
        (r6.person_id is not null) as rule_6_bp_high,
        case
            when r2.person_id is null then 'Excluded'  -- Must have HbA1c 58-75
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is not null then 'Included'
            when r6.person_id is not null then 'Included'
            else 'Excluded'  -- Rule 6 failed = exclude from final result
        end as final_status
    from diabetes_register dr
    left join pg1_exclusions pg1 on dr.person_id = pg1.person_id
    left join pg2_exclusions pg2 on dr.person_id = pg2.person_id
    left join rule_2_hba1c_prerequisite r2 on dr.person_id = r2.person_id
    left join rule_3_combined r3 on dr.person_id = r3.person_id
    left join rule_4_egfr_range r4 on dr.person_id = r4.person_id
    left join rule_5_acr_high r5 on dr.person_id = r5.person_id
    left join rule_6_bp_high r6 on dr.person_id = r6.person_id
    where pg1.person_id is null
      and pg2.person_id is null
)

select
    person_id,
    final_status,
    'Diabetes' as condition,
    '3a' as priority_group,
    'MRa' as risk_group
from patient_rules
where final_status = 'Included'
