-- LTC LCS: COPD Register - Priority Group 2 (High Risk)
-- Implements inclusion/exclusion rules for high-risk patient identification
-- Parent population: COPD register, excluding PG1 (HRC)

-- Inclusion rules (any one qualifies):
-- - Rule 2: latest FEV1 < 50 (inclusion)
-- - Rule 3: Chronic cor pulmonale within last 5 years (inclusion)
-- - Rule 4: MRC Breathlessness Scale: grade 4 (inclusion)
-- - Rule 5: 2+ COPD exacerbations in past year, Acute exacerbation of COPD


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
-- Rule 2: latest FEV1 < 50 (inclusion)
rule_2_fev1_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_copd_reg_pg2_hr_vs1") }})
    where result_value < 50
),
-- Rule 3: Chronic cor pulmonale within last 5 years (inclusion)
rule_3_chronic_cor_pulmonale as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg2_hr_vs2") }})
    where clinical_effective_date >= dateadd(month, -5*12, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 4: MRC Breathlessness Scale: grade 4 (inclusion)
rule_4_mrcbs_grade_4 as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg2_hr_vs3") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 5: 1+ COPD exacerbations in past year
rule_5_copd_exacerbations as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg2_hr_vs4") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Combine rule results for all COPD register patients
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is not null) as rule_2_fev1_low,
        (r3.person_id is not null) as rule_3_chronic_cor_pulmonale,
        (r4.person_id is not null) as rule_4_mrcbs_grade_4,
        (r5.person_id is not null) as rule_5_copd_exacerbations,
        case
            when r2.person_id is not null 
                or r3.person_id is not null 
                or r4.person_id is not null 
                or r5.person_id is not null  
            then 'Included' -- include patients with any of rules 2-5
            else 'Excluded'
        end as final_status
    from copd_register cr
    left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
    left join rule_2_fev1_low r2 on cr.person_id = r2.person_id
    left join rule_3_chronic_cor_pulmonale r3 on cr.person_id = r3.person_id
    left join rule_4_mrcbs_grade_4 r4 on cr.person_id = r4.person_id
    left join rule_5_copd_exacerbations r5 on cr.person_id = r5.person_id
    where pg1.person_id is null -- exlude PG1 patients
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'COPD' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'