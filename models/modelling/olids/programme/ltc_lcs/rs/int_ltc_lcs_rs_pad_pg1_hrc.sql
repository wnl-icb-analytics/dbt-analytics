-- LTC LCS: PAD Register - Priority Group 1 (High Risk & Complex)
-- Parent population: PAD register
-- EMIS source: 'On PAD Register- LTC LCS Priority Group 1 (HRC)'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/pad/on_pad_register_ltc_lcs_priority_group_1_hrc.md)
--
-- Rule chain:
-- - Rule 1: peripheral ischaemia code (vs1) in last 90 days -> include

with
-- Parent population: Patients currently on PAD register
pad_register as (
    select distinct person_id
    from {{ ref('fct_person_pad_register') }}
    where is_on_register = true
),

-- Rule 1: peripheral ischaemia code in last 90 days
rule_1_recent_ischaemia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_pad_reg_pg1_hrc_vs1") }})
    where clinical_effective_date >= dateadd(day, -90, current_date())
),

-- Combine rule results for all PAD register patients
patient_rules as (
    select
        pr.person_id,
        (r1.person_id is not null) as rule_1_recent_ischaemia,
        case
            when r1.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from pad_register pr
    left join rule_1_recent_ischaemia r1 on pr.person_id = r1.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'PAD' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
