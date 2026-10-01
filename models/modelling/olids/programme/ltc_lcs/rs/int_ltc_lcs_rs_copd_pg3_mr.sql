-- LTC LCS: COPD Register - Priority Group 3 (Medium Risk)
-- Implements inclusion/exclusion rules for medium-risk patient identification
-- Parent population: COPD register, excluding PG1 (HRC) and PG2 (HR)

-- Inclusion rules (any one qualifies):
-- - Rule 2: FEV1 >= 50 and < 80 (inclusion)
-- - Rule 3: MRC Breathlessness Scale: grade 2 or 3 (inclusion)
-- - Rule 4: 1 COPD exacerbation within last 12 months OR 
          -- medication issues (any of Amoxicillin, Amoxicillin Trihydrate,
          -- Doxycycline, Doxycycline Hyclate, Doxycycline Monohydrate, Erythromycin, 
          -- Erythromycin (As Stearate), Erythromycin Ethyl Succinate, Clarithromycin) 
          -- within last 12 months OR
          -- Medication issues (any of Prednisolone, Prednisolone Sodium Phosphate, 
          -- Prednisolone Steaglate) within last 12 months OR 
          -- Medication issues (any of Azithromycin, Azithromycin) within last 12 months
-- - Rule 5: Medication issues (any of Trimbow, Trelegy Ellipta) within last 6 months
-- - Rule 6: Medication issues (any of Beclometasone Dipropionate, Budesonide, Ciclesonide, 
          -- Fluticasone Furoate, Fluticasone Propionate, Mometasone Furoate) within last 6 months AND
          -- Medication issues (any of Aclidinium Bromide, Glycopyrronium bromide 55microgram 
          -- inhalation powder capsules with device, Seebri Breezhaler 44microgram inhalation 
          -- powder capsules with device (Novartis Pharmaceuticals UK Ltd) ...etc) within last 6 months AND
          -- Medication issues ( Bambuterol Hydrochloride, Formoterol Fumarate, Indacaterol, Olodaterol, 
          -- Salmeterol Xinafoate, Umeclidinium Bromide, Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose dry powder inhaler, 
          -- Beclometasone 100micrograms/dose / Formoterol 6micrograms/dose inhaler CFC free ...etc) within last 6 months

with
-- Parent population: Patients currently on copd register
copd_register as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_copd_register') }} -- LTC LCS COPD register (EMIS definition)
),
-- Rule 1: Exclude patients already in PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_copd_pg1_hrc') }}
),
-- Rule 1: Exclude patients already in PG2 (HR)
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_copd_pg2_hr') }}
),
-- Rule 2: FEV1 >= 50 and < 80 (inclusion)
rule_2_fev1_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_copd_reg_pg3_mr_vs1") }})
    where result_value >= 50 and result_value < 80
),
-- Rule 3: MRC Breathlessness Scale: grade 2 or 3 (inclusion)
rule_3_mrcbs_grade_2_3 as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg3_mr_vs2") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 4a: 1+ COPD exacerbation within last 12 months.
-- Re-using the COPD ruleset for COPD exacerbation from pg2 - the code in ruleset on_copd_reg_pg3_mr_vs3 is only in
-- EMIS's local nameset and doesn't match, returning 0 results.
rule_4a_copd_exacerbations as ( 
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg2_hr_vs4") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 4b: Medication issues (antibiotics) - 2 or more within last 12 months
rule_4b_medication_issues as ( 
    select person_id, count(*) as issue_count
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs4") }})
    where order_date >= dateadd(month, -12, current_date())
    group by all
    having count(*) >= 2
),
-- Rule 4c: Medication issues (steroids) within last 12 months
rule_4c_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs5") }})
    where order_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
-- Rule 4d: Medication issues (Azithromycin) within last 12 months
rule_4d_medication_issues as ( 
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs6") }})
    where order_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
-- Rule 5: Medication issues (Trimbow/Trelegy Ellipta inhalers) within last 6 months
rule_5_medication_issues as (
    select person_id
    from ({{ get_medication_orders(cluster_id='COPD_TRIPLE_THERAPY_INHALERS') }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
-- Rule 6a: Medication issues (steroids) within last 6 months
rule_6a_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs8") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
-- Rule 6b: Medication issues (antimuscarinics) within last 6 months
rule_6bi_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs10") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
rule_6bii_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs9") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
rule_6biii_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs11") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
-- Rule 6c: Medication issues (LABA and steroids) within last 6 months
rule_6ci_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs12") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
rule_6cii_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs13") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
rule_6ciii_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs14") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
rule_6civ_medication_issues as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_copd_reg_pg3_mr_vs15") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1    
),
-- Combine rule results for all COPD register patients
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is not null) as rule_2_fev1_low,
        (r3.person_id is not null) as rule_3_mrcbs_grade_2_3,
        (r4a.person_id is not null) as rule_4a_copd_exacerbations,
        (r4b.person_id is not null) as rule_4b_medication_issues,
        (r4c.person_id is not null) as rule_4c_medication_issues,
        (r4d.person_id is not null) as rule_4d_medication_issues,
        (r5.person_id is not null) as rule_5_medication_issues,
        (r6a.person_id is not null) as rule_6a_medication_issues,
        (r6bi.person_id is not null) as rule_6bi_medication_issues,
        (r6bii.person_id is not null) as rule_6bii_medication_issues,
        (r6biii.person_id is not null) as rule_6biii_medication_issues,
        (r6ci.person_id is not null) as rule_6ci_medication_issues,
        (r6cii.person_id is not null) as rule_6cii_medication_issues,
        (r6ciii.person_id is not null) as rule_6ciii_medication_issues,
        (r6civ.person_id is not null) as rule_6civ_medication_issues,
        case
            when r2.person_id is not null then 'Included' -- Rule 2: FEV1 >= 50 and < 80 (inclusion)
            when r3.person_id is not null then 'Included' -- Rule 3: MRC Breathlessness Scale: grade 2 or 3 (inclusion)
            when r4a.person_id is not null or r4b.person_id is not null or r4c.person_id is not null or r4d.person_id is not null then 'Included'  -- Rule 4c: Medication issues (steroids) within last 12 months - any of 
            when r5.person_id is not null then 'Included'  -- Rule 5: Medication issues (Trimbow/Trelegy Ellipta inhalers) within last 6 months - any of
            when r6a.person_id is not null AND
                (r6bi.person_id is not null or r6bii.person_id is not null or r6biii.person_id is not null) AND
                (r6ci.person_id is not null or r6cii.person_id is not null or r6ciii.person_id is not null or r6civ.person_id is not null) -- Rule 6c: Medication issues (LABA and steroids) within last 6 months - requires one of each from three medication lists
            then 'Included' -- include patients with any of rules 2-5
            else 'Excluded'
        end as final_status
    from copd_register cr
    left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
    left join pg2_exclusions pg2 on cr.person_id = pg2.person_id
    left join rule_2_fev1_low r2 on cr.person_id = r2.person_id
    left join rule_3_mrcbs_grade_2_3 r3 on cr.person_id = r3.person_id
    left join rule_4a_copd_exacerbations r4a on cr.person_id = r4a.person_id
    left join rule_4b_medication_issues r4b on cr.person_id = r4b.person_id
    left join rule_4c_medication_issues r4c on cr.person_id = r4c.person_id
    left join rule_4d_medication_issues r4d on cr.person_id = r4d.person_id
    left join rule_5_medication_issues r5 on cr.person_id = r5.person_id
    left join rule_6a_medication_issues r6a on cr.person_id = r6a.person_id
    left join rule_6bi_medication_issues r6bi on cr.person_id = r6bi.person_id 
    left join rule_6bii_medication_issues r6bii on cr.person_id = r6bii.person_id
    left join rule_6biii_medication_issues r6biii on cr.person_id = r6biii.person_id
    left join rule_6ci_medication_issues r6ci on cr.person_id = r6ci.person_id
    left join rule_6cii_medication_issues r6cii on cr.person_id = r6cii.person_id
    left join rule_6ciii_medication_issues r6ciii on cr.person_id = r6ciii.person_id
    left join rule_6civ_medication_issues r6civ on cr.person_id = r6civ.person_id
    where 
    pg1.person_id is null -- exlude PG1 patients
    and pg2.person_id is null -- exclude PG2 patients
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'COPD' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'