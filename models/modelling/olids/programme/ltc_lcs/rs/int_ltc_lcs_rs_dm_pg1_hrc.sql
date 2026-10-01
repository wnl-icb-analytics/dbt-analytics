-- LTC LCS: Diabetes Register - Priority Group 1 (High Risk & Complex)
-- Implements inclusion/exclusion rules for high-risk & complex patient identification
-- Parent population: Diabetes register

with
-- Parent population: Patients currently on diabetes register
diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

-- Rule 1: HbA1c > 90 (inclusion)
rule_1_hba1c_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg1_hrc_vs1") }})
    where result_value > 90
),

-- Rule 2: eGFR < 15 (inclusion)
rule_2_egfr_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg1_hrc_vs2") }})
    where result_value < 15
),

-- Rule 3: Albumin:creatinine ratio > 250 (inclusion)
rule_3_acr_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg1_hrc_vs3") }})
    where result_value > 250
),

-- Rule 4: Enhanced Liver Fibrosis score > 9.8 (inclusion)
rule_4_elf_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg1_hrc_vs4") }})
    where result_value > 9.8
),

-- Rule 5: HbA1c > 75 (prerequisite for Rule 6)
rule_5_hba1c_prerequisite as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg1_hrc_vs1") }})
    where result_value > 75
),

-- Rule 6: Heart failure (inclusion) - episode type not Review or Ended
rule_6_cardiac as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_pg1_hrc_vs5") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- Rule 6: Diabetes medications in last 6 months (inclusion)
-- GLP-1 agonists (vs7) or Insulins
-- Note: vs6 is empty (EMIS DRUG Group didn't resolve to SNOMED codes)
-- Using BNF code 060101 (Insulins) instead
rule_6_medications as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_dm_reg_pg1_hrc_vs7") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_medication_orders(bnf_code='060101') }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),

-- Rule 6: Only applies if Rule 5 passed (HbA1c > 75)
rule_6_with_prerequisite as (
    select person_id
    from rule_6_cardiac
    where person_id in (select person_id from rule_5_hba1c_prerequisite)
    union
    select person_id
    from rule_6_medications
    where person_id in (select person_id from rule_5_hba1c_prerequisite)
),

-- Combine rule results for all diabetes register patients
patient_rules as (
    select
        dr.person_id,
        (r1.person_id is not null) as rule_1_hba1c_high,
        (r2.person_id is not null) as rule_2_egfr_low,
        (r3.person_id is not null) as rule_3_acr_high,
        (r4.person_id is not null) as rule_4_elf_high,
        (r5.person_id is not null) as rule_5_hba1c_prerequisite,
        (r6.person_id is not null) as rule_6_passed,
        case
            when r1.person_id is not null or r2.person_id is not null or r3.person_id is not null or r4.person_id is not null then 'Included'  -- Rules 1-4 are independent
            when r5.person_id is not null and r6.person_id is not null then 'Included'  -- Rule 6 only applies if Rule 5 passes
            else 'Excluded'
        end as final_status
    from diabetes_register dr
    left join rule_1_hba1c_high r1 on dr.person_id = r1.person_id
    left join rule_2_egfr_low r2 on dr.person_id = r2.person_id
    left join rule_3_acr_high r3 on dr.person_id = r3.person_id
    left join rule_4_elf_high r4 on dr.person_id = r4.person_id
    left join rule_5_hba1c_prerequisite r5 on dr.person_id = r5.person_id
    left join rule_6_with_prerequisite r6 on dr.person_id = r6.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Diabetes' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'