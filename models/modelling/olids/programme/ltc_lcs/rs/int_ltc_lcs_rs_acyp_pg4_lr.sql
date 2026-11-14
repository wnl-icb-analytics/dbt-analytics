-- LTC LCS: Asthma CYP Register - Priority Group 4 (Low Risk)
-- Parent population: CYP asthma register ONLY (not on any other LTC LCS
-- register), excluding PG2 (HR)
-- EMIS source: 'On Asthma(CYP) Register- LTC LCS Priority Group 4 (LR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/asthma_cyp/on_asthma_cyp_register_ltc_lcs_priority_group_4_lr.md)
--
-- Rule chain:
-- - Rule 1: include if not in PG2 (HR), else exclude
--
-- The EMIS LR search excludes the PG2 'V2' search; this model excludes the
-- newer 'V2 Oct 2025' variant implemented in int_ltc_lcs_rs_acyp_pg2_hr.

with
-- Parent population: CYP asthma register, excluding patients on any other
-- LTC LCS register ('LTC LCS: Asthma CYP Register ONLY')
cyp_asthma_only as (
    select distinct r.person_id
    from {{ ref('fct_person_cyp_asthma_register') }} r
    where r.is_on_register = true
        and r.person_id not in (
            select person_id from {{ ref('fct_person_atrial_fibrillation_register') }}
            union
            select person_id from {{ ref('fct_person_ckd_register') }}
            union
            select person_id from {{ ref('fct_person_chd_register') }}
            union
            select person_id from {{ ref('fct_person_diabetes_register') }}
            union
            select person_id from {{ ref('fct_person_hypertension_register') }}
            union
            select person_id from {{ ref('int_ltc_lcs_nafld_register') }}
            union
            select r2.person_id
            from {{ ref('fct_person_asthma_register') }} r2
            inner join {{ ref('dim_person_age') }} a2 on r2.person_id = a2.person_id
            where a2.age >= 18
            union
            select person_id from {{ ref('int_ltc_lcs_copd_register') }}
            union
            select person_id from {{ ref('fct_person_heart_failure_register') }}
            union
            select person_id from {{ ref('fct_person_pad_register') }}
            union
            select person_id from {{ ref('fct_person_stroke_tia_register') }}
        )
),

-- Rule 1: Exclude PG2 (HR)
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_acyp_pg2_hr') }}
),

patient_rules as (
    select
        cr.person_id,
        (pg2.person_id is not null) as rule_1_in_pg2,
        case
            when pg2.person_id is null then 'Included'
            else 'Excluded'
        end as final_status
    from cyp_asthma_only cr
    left join pg2_exclusions pg2 on cr.person_id = pg2.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Asthma CYP' as condition,
    '4' as priority_group,
    'LR' as risk_group
from patient_rules
where final_status = 'Included'
