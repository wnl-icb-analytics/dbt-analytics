-- LTC LCS: AF Register - Priority Group 2 (High Risk)
-- Parent population: AF register
-- EMIS source: 'On AF Register- LTC LCS Priority Group 2 (HR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/af/on_af_register_ltc_lcs_priority_group_2_hr.md)
--
-- Rule chain:
-- - Rule 1 (gate): anticoagulant therapy in last 6 months - oral anticoagulants (vs1),
--   DOAC substances (vs2), DOAC products (vs3) or 'anticoagulant prescribed by third
--   party' code (vs4). Fail -> exclude.
-- - Rule 2: antiplatelet order (vs5) or aspirin prophylaxis code (vs6) in last 6 months -> include
-- - Rule 3: latest HAS-BLED score (vs7) >= 3 or latest ORBIT-AF score (vs8) >= 4 -> include
-- - Rule 4 (gate): DOAC order (vs2) in last 6 months. Fail -> exclude.
-- - Rule 5: latest Cockcroft-Gault (vs9) < 40, latest body weight (vs10) < 50,
--   any Rockwood level 7-9 code (vs11) or latest Rockwood score (vs12) >= 7 -> include
--
-- Net logic: rule_1 and (rule_2 or rule_3 or (rule_4 and rule_5))
--
-- vs1 note: the 'Oral Anticoagulants' dm+d drug group (29711000033114, SCT_DRGGRP)
-- is unexpanded in the reference tables (expansion gap), so the vs1 arm is
-- supplemented with BNF section 020802 (oral anticoagulants) via
-- get_medication_orders, which covers warfarin-type VKAs the valueset misses.

with
-- Parent population: Patients currently on AF register
af_register as (
    select distinct person_id
    from {{ ref('fct_person_atrial_fibrillation_register') }}
    where is_on_register = true
),

-- Rule 1: anticoagulant therapy in last 6 months
rule_1_anticoagulants as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_af_reg_pg2_hr_vs1, on_af_reg_pg2_hr_vs2, on_af_reg_pg2_hr_vs3") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    -- vs1 supplement: oral anticoagulants via BNF 2.8.2 (warfarin-type VKAs;
    -- the dm+d drug group behind vs1 is unexpanded)
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='020802') }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations("on_af_reg_pg2_hr_vs4") }})
    where clinical_effective_date >= dateadd(month, -6, current_date())
),

-- Rule 2: antiplatelet therapy or aspirin prophylaxis in last 6 months
rule_2_antiplatelets as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_af_reg_pg2_hr_vs5") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations("on_af_reg_pg2_hr_vs6") }})
    where clinical_effective_date >= dateadd(month, -6, current_date())
),

-- Rule 3: latest HAS-BLED >= 3 or latest ORBIT-AF >= 4
rule_3_bleeding_risk as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg2_hr_vs7") }})
    where result_value >= 3
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg2_hr_vs8") }})
    where result_value >= 4
),

-- Rule 4: DOAC order in last 6 months
rule_4_doacs as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_af_reg_pg2_hr_vs2") }})
    where order_date >= dateadd(month, -6, current_date())
),

-- Rule 5: renal impairment, low body weight or severe frailty
rule_5_renal_frailty as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg2_hr_vs9") }})
    where result_value < 40
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg2_hr_vs10") }})
    where result_value < 50
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_af_reg_pg2_hr_vs11") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg2_hr_vs12") }})
    where result_value >= 7
),

-- Combine rule results for all AF register patients
patient_rules as (
    select
        afr.person_id,
        (r1.person_id is not null) as rule_1_anticoagulants,
        (r2.person_id is not null) as rule_2_antiplatelets,
        (r3.person_id is not null) as rule_3_bleeding_risk,
        (r4.person_id is not null) as rule_4_doacs,
        (r5.person_id is not null) as rule_5_renal_frailty,
        case
            when r1.person_id is not null
                and (
                    r2.person_id is not null
                    or r3.person_id is not null
                    or (r4.person_id is not null and r5.person_id is not null)
                )
                then 'Included'
            else 'Excluded'
        end as final_status
    from af_register afr
    left join rule_1_anticoagulants r1 on afr.person_id = r1.person_id
    left join rule_2_antiplatelets r2 on afr.person_id = r2.person_id
    left join rule_3_bleeding_risk r3 on afr.person_id = r3.person_id
    left join rule_4_doacs r4 on afr.person_id = r4.person_id
    left join rule_5_renal_frailty r5 on afr.person_id = r5.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'AF' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
