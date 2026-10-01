-- LTC LCS: NAFLD Register - Priority Group 3 (Medium Risk)
-- Parent population: NAFLD register
-- EMIS source: 'On NAFLD Register- LTC LCS Priority Group 3 (MR)'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/nafld/on_nafld_register_ltc_lcs_priority_group_3_mr.md)
--
-- Rule chain (latest = latest reading within the last 3 years):
-- - Rule 1: latest NAFLD fibrosis score (vs1) > 3.25 -> include
-- - Rule 2: latest NAFLD fibrosis score > 1.3 and <= 3.25 and latest
--   ELF score (vs2) >= 9.8 -> include, else exclude
--
with
-- Parent population: Patients currently on the LTC LCS NAFLD register
-- (the EMIS NAFLD Register v2 definition; broader than the MASLD-cluster
-- fct_person_nafld_register)
nafld_register as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_nafld_register') }}
),

-- Latest NAFLD fibrosis score within the last 3 years
latest_fibrosis_score as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("on_nafld_reg_pg3_mr_vs1") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

-- Latest ELF score within the last 3 years
latest_elf_score as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("on_nafld_reg_pg3_mr_vs2") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

-- Combine rule results for all NAFLD register patients
patient_rules as (
    select
        nr.person_id,
        (fib.result_value > 3.25) as rule_1_fibrosis_high,
        (
            fib.result_value > 1.3 and fib.result_value <= 3.25
            and elf.result_value >= 9.8
        ) as rule_2_fibrosis_indeterminate_elf_high,
        case
            when fib.result_value > 3.25
                or (
                    fib.result_value > 1.3 and fib.result_value <= 3.25
                    and elf.result_value >= 9.8
                )
                then 'Included'
            else 'Excluded'
        end as final_status
    from nafld_register nr
    left join latest_fibrosis_score fib on nr.person_id = fib.person_id
    left join latest_elf_score elf on nr.person_id = elf.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'NAFLD' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
