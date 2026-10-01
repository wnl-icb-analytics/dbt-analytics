-- LTC LCS: Asthma CYP Register - Priority Group 2 (High Risk)
-- Parent population: CYP asthma register ONLY (not on any other LTC LCS register)
-- EMIS source: 'On Asthma(CYP) Register- LTC LCS Priority Group 2 (HR)* V2 Oct 2025'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/asthma_cyp/on_asthma_cyp_register_ltc_lcs_priority_group_2_hr_v2_oct_2025.md)
-- Latest spec variant implemented (supersedes V1 and V2).
--
-- Rule chain:
-- - Rule 1: latest child protection status (vs1) is 'Child on protection
--   register' (vs2), or latest child-in-need status (vs3) is 'Child in need'
--   (vs4) -> include
-- - Rule 2: emergency asthma admission code (vs5) in last 12 months -> include
-- - Rule 3: > 2 acute exacerbation codes (vs6) in last 12 months -> include
-- - Rule 4: >= 2 prednisolone (vs7) orders in last 12 months -> include
-- - Rule 5: theophylline/aminophylline (vs8) order in last 12 months -> include
-- - Rule 6: >= 3 SABA (vs9) orders in last 3 months -> include
-- - Rule 7: LABA (vs10 bambuterol, vs11 formoterol products) in last 12 months -> include
-- - Rule 8: no ICS (vs12) in last 12 months and SABA (vs9) in last 3 months
--   -> include, else exclude
--
-- Issue-count note: the agent-API parse of the R5 XML drops issue-count
-- restrictions; counts implemented per the live EMIS search definitions
-- (issue #396), consistent with the adult asthma models.

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

-- Rule 1: latest child protection status is 'on protection register', or
-- latest child-in-need status is 'in need'. vs2/vs4 are subsets of vs1/vs3,
-- so the status check matches on observation_id.
rule_1_safeguarding as (
    select latest_protection.person_id
    from ({{ get_ltc_lcs_observations_latest("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs1") }}) latest_protection
    inner join ({{ get_ltc_lcs_observations("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs2") }}) on_register
        on latest_protection.observation_id = on_register.observation_id
    union
    select latest_in_need.person_id
    from ({{ get_ltc_lcs_observations_latest("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs3") }}) latest_in_need
    inner join ({{ get_ltc_lcs_observations("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs4") }}) in_need
        on latest_in_need.observation_id = in_need.observation_id
),

-- Rule 2: emergency asthma admission code in last 12 months
rule_2_emergency_admission as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs5") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

-- Rule 3: more than 2 acute exacerbation codes in last 12 months
rule_3_exacerbation as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs6") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
    group by person_id
    having count(distinct observation_id) > 2
),

-- Rule 4: >= 2 prednisolone orders in last 12 months
rule_4_prednisolone as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs7") }})
    where order_date >= dateadd(month, -12, current_date())
    group by person_id
    having count(distinct medication_order_id) >= 2
),

-- Rule 5: theophylline order in last 12 months
rule_5_theophylline as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs8") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Rule 6: >= 3 SABA orders in last 3 months
rule_6_saba as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9") }})
    where order_date >= dateadd(month, -3, current_date())
    group by person_id
    having count(distinct medication_order_id) >= 3
),

-- Rule 7: LABA order in last 12 months
rule_7_laba as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs10, on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs11") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Rule 8 arms: any SABA order in last 3 months, no ICS order in last 12 months
saba_any_3m as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs9") }})
    where order_date >= dateadd(month, -3, current_date())
),

ics_12m as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_cyp_reg_pg2_hr_v2_oct_2025_vs12") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Combine rule results for all CYP-only asthma register patients
patient_rules as (
    select
        cr.person_id,
        (r1.person_id is not null) as rule_1_safeguarding,
        (r2.person_id is not null) as rule_2_emergency_admission,
        (r3.person_id is not null) as rule_3_exacerbation,
        (r4.person_id is not null) as rule_4_prednisolone,
        (r5.person_id is not null) as rule_5_theophylline,
        (r6.person_id is not null) as rule_6_saba,
        (r7.person_id is not null) as rule_7_laba,
        (s3.person_id is not null and i12.person_id is null) as rule_8_saba_no_ics,
        case
            when r1.person_id is not null
                or r2.person_id is not null
                or r3.person_id is not null
                or r4.person_id is not null
                or r5.person_id is not null
                or r6.person_id is not null
                or r7.person_id is not null
                or (s3.person_id is not null and i12.person_id is null)
                then 'Included'
            else 'Excluded'
        end as final_status
    from cyp_asthma_only cr
    left join rule_1_safeguarding r1 on cr.person_id = r1.person_id
    left join rule_2_emergency_admission r2 on cr.person_id = r2.person_id
    left join rule_3_exacerbation r3 on cr.person_id = r3.person_id
    left join rule_4_prednisolone r4 on cr.person_id = r4.person_id
    left join rule_5_theophylline r5 on cr.person_id = r5.person_id
    left join rule_6_saba r6 on cr.person_id = r6.person_id
    left join rule_7_laba r7 on cr.person_id = r7.person_id
    left join saba_any_3m s3 on cr.person_id = s3.person_id
    left join ics_12m i12 on cr.person_id = i12.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Asthma CYP' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
