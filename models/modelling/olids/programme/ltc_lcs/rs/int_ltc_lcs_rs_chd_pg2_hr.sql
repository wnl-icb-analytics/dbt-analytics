-- LTC LCS: CHD Register - Priority Group 2 (High Risk)
-- Implements inclusion/exclusion rules for high-risk identification
-- Parent population: CHD register
--
-- Inclusion rules:
-- - Rule 1: Not in PG1 HRC
-- - Rule 2: CHD not recorded more than one year ago
-- - Rule 3: CHD recorded within the last 1 year to before 1 month ago

with
-- Parent population: Patients currently on CHD register
chd_register as (
    select distinct person_id
    from {{ ref('fct_person_chd_register') }}
),
-- Rule 1: Exclude patients already in PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_chd_pg1_hrc') }}
),
-- Rule 2: CHD not recorded more than one year ago
rule_2_chd_not_over_one_year_ago as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg2_hr_vs1") }})
    where
    clinical_effective_date < dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 3: CHD recorded within the last 1 year to before 1 month ago
rule_3_chd_in_last_year_excl_last_month as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg2_hr_vs1") }})
    where
    clinical_effective_date > dateadd(month, -12, current_date())
    and clinical_effective_date < dateadd(month, -1, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Combine rule results for all CHD register patients
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is null) as rule_2_chd_not_over_one_year_ago,
        (r3.person_id is not null) as rule_3_chd_in_last_year_excl_last_month,
        case
            when r1.person_id is null
                and r2.person_id is null
                and r3.person_id is not null
            then 'Included'
            else 'Excluded'
        end as final_status
    from chd_register cr
    left join pg1_exclusions r1 on cr.person_id = r1.person_id
    left join rule_2_chd_not_over_one_year_ago r2 on cr.person_id = r2.person_id
    left join rule_3_chd_in_last_year_excl_last_month r3 on cr.person_id = r3.person_id
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'CHD' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
