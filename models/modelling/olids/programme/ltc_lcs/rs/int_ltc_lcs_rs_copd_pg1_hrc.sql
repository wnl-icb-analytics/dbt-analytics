-- LTC LCS: COPD Register - Priority Group 1 (High Risk & Complex)
-- Implements inclusion/exclusion rules for high-risk & complex patient identification
-- Parent population: COPD register

with
-- Parent population: Patients currently on copd register
copd_register as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_copd_register') }} -- LTC LCS COPD register (EMIS definition)
),
-- Rule 1a: latest FEV1 < 30 (inclusion)
rule_1a_fev1_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_copd_reg_pg1_hrc_vs1") }})
    where result_value < 30
),
-- Rule 1b: MRC Breathlessness Scale: grade 5 (inclusion)
rule_1b_mrcbs_grade_5 as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg1_hrc_vs2") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 1c: Home oxygen supply or oxygen therapy in last 12 months (inclusion)
rule_1c_oxygen_therapy as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_copd_reg_pg1_hrc_vs3") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Combine rule results for all COPD register patients
patient_rules as (
    select
        cr.person_id,
        (r1a.person_id is not null) as rule_1a_fev1_low,
        (r1b.person_id is not null) as rule_1b_mrcbs_grade_5,
        (r1c.person_id is not null) as rule_1c_oxygen_therapy,
        case
            when r1a.person_id is not null or r1b.person_id is not null or r1c.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from copd_register cr
    left join rule_1a_fev1_low r1a on cr.person_id = r1a.person_id
    left join rule_1b_mrcbs_grade_5 r1b on cr.person_id = r1b.person_id
    left join rule_1c_oxygen_therapy r1c on cr.person_id = r1c.person_id
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'COPD' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'