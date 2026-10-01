-- LTC LCS: Asthma Adult Register - Priority Group 4 (Low Risk)
-- Parent population: Asthma register, age >= 18, excluding PG1-PG3
-- EMIS source: 'On Asthma(Adult) Register- LTC LCS Priority Group 4 (LR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/asthma_adult/on_asthma_adult_register_ltc_lcs_priority_group_4_lr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC), PG2 (HR) and PG3 (MR) patients
-- - Rule 2: ICS (vs1) order in last 6 months -> include
-- - Rule 3: SABA (vs2) order in last 12 months -> include, else exclude
--
-- Parent register note: the EMIS parent 'LTC LCS: Asthma Adult Register*' is
-- the asthma register restricted to age >= 18; fct_person_asthma_register is
-- age >= 6, so the age filter is applied here.

with
-- Parent population: Adults currently on asthma register
asthma_adult_register as (
    select distinct r.person_id
    from {{ ref('fct_person_asthma_register') }} r
    inner join {{ ref('dim_person_age') }} a on r.person_id = a.person_id
    where r.is_on_register = true
        and a.age >= 18
),

-- Rule 1: Exclude PG1 (HRC), PG2 (HR) and PG3 (MR)
pg1_to_pg3_exclusions as (
    select distinct person_id from {{ ref('int_ltc_lcs_rs_ast_pg1_hrc') }}
    union
    select distinct person_id from {{ ref('int_ltc_lcs_rs_ast_pg2_hr') }}
    union
    select distinct person_id from {{ ref('int_ltc_lcs_rs_ast_pg3_mr') }}
),

-- Rule 2: ICS order in last 6 months
rule_2_ics as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg4_lr_vs1") }})
    where order_date >= dateadd(month, -6, current_date())
),

-- Rule 3: SABA order in last 12 months
rule_3_saba as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg4_lr_vs2") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Combine rule results for all adult asthma register patients
patient_rules as (
    select
        ar.person_id,
        (excl.person_id is not null) as rule_1_in_pg1_to_pg3,
        (r2.person_id is not null) as rule_2_ics,
        (r3.person_id is not null) as rule_3_saba,
        case
            when excl.person_id is not null then 'Excluded'
            when r2.person_id is not null or r3.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from asthma_adult_register ar
    left join pg1_to_pg3_exclusions excl on ar.person_id = excl.person_id
    left join rule_2_ics r2 on ar.person_id = r2.person_id
    left join rule_3_saba r3 on ar.person_id = r3.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Asthma Adult' as condition,
    '4' as priority_group,
    'LR' as risk_group
from patient_rules
where final_status = 'Included'
