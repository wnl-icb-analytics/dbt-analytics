-- LTC LCS: AF Register - Priority Group 3 (Medium Risk)
-- Parent population: AF register, excluding PG2 (HR)
-- EMIS source: 'On AF Register- LTC LCS Priority Group 3 (MR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/af/on_af_register_ltc_lcs_priority_group_3_mr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG2 (HR) patients
-- - Rule 2 (gate): anticoagulant therapy in last 6 months (vs1-vs4), age > 65,
--   latest Cockcroft-Gault (vs5) 40-<60, latest body weight (vs6) >= 50,
--   latest Rockwood level 1-6 (vs7) is level 6 (vs8),
--   latest Rockwood score (vs9) 6-<7 in last 6 months,
--   or latest CHA2DS2-VASc (vs10) >= 1. Fail -> exclude.
-- - Rule 3: oral anticoagulants (vs1) in last 6 months and latest serum
--   creatinine (vs11) over 15 months old -> include
-- - Rule 4: DOAC products (vs3) in last 6 months and latest serum creatinine
--   over 15 months old -> include
-- - Rule 5: DOACs (vs3) and latest Cockcroft-Gault 40-<60 -> include
-- - Rule 6: DOACs (vs3) and latest body weight >50-<=60 -> include
-- - Rule 7: DOACs (vs3) and age >= 75 -> include
-- - Rule 8: DOACs (vs3) and latest Rockwood level 1-6 is level 6 -> include
-- - Rule 9: DOACs (vs3) and latest Rockwood score 6-<7 in last 6 months -> include
-- - Rule 10 (gate): anticoagulant therapy in last 6 months (vs1-vs4) -> exclude
--   (rules 11-14 only apply to patients not on anticoagulation)
-- - Rule 11: latest CHA2DS2-VASc >= 1 and male -> include
-- - Rule 12: latest CHA2DS2-VASc >= 2 and female -> include
-- - Rule 13: latest CHA2DS2-VASc over 2 years old and age > 65 -> include
-- - Rule 14: no CHA2DS2-VASc recorded and age > 65 -> include, else exclude
--
-- vs1 note: the 'Oral Anticoagulants' dm+d drug group (29711000033114, SCT_DRGGRP)
-- is unexpanded in the reference tables, so vs1 arms are supplemented with BNF
-- section 020802 (oral anticoagulants) via get_medication_orders, which covers
-- the warfarin-type VKAs the valueset misses.

with
-- Parent population: Patients currently on AF register
af_register as (
    select distinct person_id
    from {{ ref('fct_person_atrial_fibrillation_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG2 (HR)
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_af_pg2_hr') }}
),

-- Oral anticoagulants via BNF 2.8.2 (vs1 supplement: the dm+d drug group
-- behind vs1 is unexpanded, so warfarin-type VKAs come from BNF)
bnf_oral_anticoagulants_6m as (
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='020802') }})
    where order_date >= dateadd(month, -6, current_date())
),

-- Anticoagulant therapy in last 6 months (rule 2 arm and rule 10 gate)
anticoagulants_6m as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_af_reg_pg3_mr_vs1, on_af_reg_pg3_mr_vs2, on_af_reg_pg3_mr_vs3") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id from bnf_oral_anticoagulants_6m
    union
    select person_id
    from ({{ get_ltc_lcs_observations("on_af_reg_pg3_mr_vs4") }})
    where clinical_effective_date >= dateadd(month, -6, current_date())
),

-- Oral anticoagulants (vs1) in last 6 months (rule 3)
oral_anticoagulants_6m as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_af_reg_pg3_mr_vs1") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id from bnf_oral_anticoagulants_6m
),

-- DOAC products (vs3) in last 6 months (rules 4-9)
doacs_6m as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_af_reg_pg3_mr_vs3") }})
    where order_date >= dateadd(month, -6, current_date())
),

ages as (
    select person_id, age
    from {{ ref('dim_person_age') }}
),

genders as (
    select person_id, gender
    from {{ ref('dim_person_demographics') }}
),

-- Latest Cockcroft-Gault creatinine clearance 40-<60
cockcroft_gault_40_60 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg3_mr_vs5") }})
    where result_value >= 40 and result_value < 60
),

-- Latest body weight
latest_weight as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg3_mr_vs6") }})
),

-- Latest Rockwood level among levels 1-6 (vs7) is level 6 (vs8).
-- vs8 is a subset of vs7, so the level-6 check matches on observation_id.
rockwood_level_6 as (
    select latest_level.person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg3_mr_vs7") }}) latest_level
    inner join ({{ get_ltc_lcs_observations("on_af_reg_pg3_mr_vs8") }}) level_6
        on latest_level.observation_id = level_6.observation_id
),

-- Latest Rockwood score 6-<7 recorded in last 6 months
rockwood_score_6 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg3_mr_vs9") }})
    where result_value >= 6 and result_value < 7
        and clinical_effective_date > dateadd(month, -6, current_date())
),

-- Latest CHA2DS2-VASc score (vs10, refset)
latest_chadsvasc as (
    select person_id, result_value, clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg3_mr_vs10") }})
),

-- Latest serum creatinine (vs11)
latest_creatinine as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("on_af_reg_pg3_mr_vs11") }})
),

-- Combine rule results for all AF register patients
patient_rules as (
    select
        afr.person_id,
        (pg2.person_id is not null) as rule_1_in_pg2,
        (
            ac.person_id is not null
            or coalesce(a.age, 0) > 65
            or cg.person_id is not null
            or coalesce(w.result_value, 0) >= 50
            or rl6.person_id is not null
            or rs6.person_id is not null
            or coalesce(cv.result_value, -1) >= 1
        ) as rule_2_gate,
        (oac.person_id is not null and cr.clinical_effective_date < dateadd(month, -15, current_date())) as rule_3_oac_stale_creatinine,
        (doac.person_id is not null and cr.clinical_effective_date < dateadd(month, -15, current_date())) as rule_4_doac_stale_creatinine,
        (doac.person_id is not null and cg.person_id is not null) as rule_5_doac_cockcroft_gault,
        (doac.person_id is not null and w.result_value > 50 and w.result_value <= 60) as rule_6_doac_low_weight,
        (doac.person_id is not null and coalesce(a.age, 0) >= 75) as rule_7_doac_age_75,
        (doac.person_id is not null and rl6.person_id is not null) as rule_8_doac_rockwood_level_6,
        (doac.person_id is not null and rs6.person_id is not null) as rule_9_doac_rockwood_score_6,
        (ac.person_id is not null) as rule_10_on_anticoagulants,
        (cv.result_value >= 1 and g.gender = 'Male') as rule_11_chadsvasc_male,
        (cv.result_value >= 2 and g.gender = 'Female') as rule_12_chadsvasc_female,
        (cv.clinical_effective_date < dateadd(year, -2, current_date()) and coalesce(a.age, 0) > 65) as rule_13_chadsvasc_stale,
        (cv.person_id is null and coalesce(a.age, 0) > 65) as rule_14_no_chadsvasc
    from af_register afr
    left join pg2_exclusions pg2 on afr.person_id = pg2.person_id
    left join anticoagulants_6m ac on afr.person_id = ac.person_id
    left join oral_anticoagulants_6m oac on afr.person_id = oac.person_id
    left join doacs_6m doac on afr.person_id = doac.person_id
    left join ages a on afr.person_id = a.person_id
    left join genders g on afr.person_id = g.person_id
    left join cockcroft_gault_40_60 cg on afr.person_id = cg.person_id
    left join latest_weight w on afr.person_id = w.person_id
    left join rockwood_level_6 rl6 on afr.person_id = rl6.person_id
    left join rockwood_score_6 rs6 on afr.person_id = rs6.person_id
    left join latest_chadsvasc cv on afr.person_id = cv.person_id
    left join latest_creatinine cr on afr.person_id = cr.person_id
),

-- Apply the rule chain in EMIS order
final_status as (
    select
        person_id,
        case
            when rule_1_in_pg2 then 'Excluded'
            when not rule_2_gate then 'Excluded'
            when rule_3_oac_stale_creatinine
                or rule_4_doac_stale_creatinine
                or rule_5_doac_cockcroft_gault
                or rule_6_doac_low_weight
                or rule_7_doac_age_75
                or rule_8_doac_rockwood_level_6
                or rule_9_doac_rockwood_score_6
                then 'Included'
            when rule_10_on_anticoagulants then 'Excluded'
            when coalesce(rule_11_chadsvasc_male, false)
                or coalesce(rule_12_chadsvasc_female, false)
                or coalesce(rule_13_chadsvasc_stale, false)
                or rule_14_no_chadsvasc
                then 'Included'
            else 'Excluded'
        end as final_status
    from patient_rules
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'AF' as condition,
    '3' as priority_group,
    'MR' as risk_group
from final_status
where final_status = 'Included'
