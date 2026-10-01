-- LTC LCS: Asthma Adult Register - Priority Group 1 (High Risk & Complex)
-- Parent population: Asthma register, age >= 18
-- EMIS source: 'On Asthma(Adult) Register- LTC LCS Priority Group 1 (HRC)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/asthma_adult/on_asthma_adult_register_ltc_lcs_priority_group_1_hrc.md)
--
-- Rule chain:
-- - Rule 1: biologic order (vs1: omalizumab, mepolizumab, reslizumab,
--   benralizumab) in last 12 months -> include
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

-- Rule 1: biologic order in last 12 months
rule_1_biologics as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg1_hrc_vs1") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Combine rule results for all adult asthma register patients
patient_rules as (
    select
        ar.person_id,
        (r1.person_id is not null) as rule_1_biologics,
        case
            when r1.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from asthma_adult_register ar
    left join rule_1_biologics r1 on ar.person_id = r1.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Asthma Adult' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
