-- LTC LCS: CKD Register - Priority Group 2 (High Risk)
-- Parent population: CKD register, excluding PG1 (HRC)
--
-- Inclusion: any of rules 2-5, or rule 6 AND (rule 7 OR rule 8).
-- BP checks use one latest event across settings; the XML has separate clinic and
-- home/ambulatory BP chains (rules 7-10).
-- - Rule 2: eGFR 45-59 AND ACR > 30
-- - Rule 3: eGFR 30-44 AND ACR > 3
-- - Rule 4: eGFR 15-29
-- - Rule 5: ACR 70-250 (>=70 and <250)
-- - Rule 6: 3+ repeat antihypertensive prescriptions in last 3 months (gate only)
-- - Rule 7: Latest BP in last 12 months with diastolic > 90
-- - Rule 8: Latest BP in last 12 months with systolic > 150

with
-- Parent population: Patients on CKD register
ckd_register as (
    select distinct person_id
    from {{ ref('fct_person_ckd_register') }}
),

-- Rule 1: Exclude patients already in PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg1_hrc') }}
),

-- Rule 2: eGFR 45-59 AND ACR > 30 (compound - both required)
-- vs1 = eGFR codes
rule_2_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs1") }})
    where result_value >= 45 and result_value <= 59
),
-- vs2 = ACR codes
rule_2_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs2") }})
    where result_value > 30
),
rule_2_combined as (
    select e.person_id
    from rule_2_egfr e
    inner join rule_2_acr a on e.person_id = a.person_id
),

-- Rule 3: eGFR 30-44 AND ACR > 3 (compound - both required)
rule_3_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs1") }})
    where result_value >= 30 and result_value <= 44
),
rule_3_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs2") }})
    where result_value > 3
),
rule_3_combined as (
    select e.person_id
    from rule_3_egfr e
    inner join rule_3_acr a on e.person_id = a.person_id
),

-- Rule 4: eGFR 15-29 (latest value)
rule_4_egfr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs1") }})
    where result_value >= 15 and result_value <= 29
),

-- Rule 5: ACR 70-250 (>=70 and <250, as 250+ is PG1 HRC)
rule_5_acr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs2") }})
    where result_value >= 70 and result_value < 250
),

-- Rule 6: 3+ repeat antihypertensive prescriptions in last 3 months (gate for BP rules)
-- EMIS requires at least three repeat issues. The project antihypertensive list
-- remains broader than the EMIS drug group; the count is not a proxy for that gap.
rule_6_antihypertensive as (
    select am.person_id
    from {{ ref('int_antihypertensive_medications_all') }} am
    inner join {{ ref('stg_olids_medication_statement') }} ms
        on am.medication_statement_id = ms.id
    where am.order_date > dateadd(month, -3, current_date())
        and ms.authorisation_type_source_code = 'Repeat'
    group by am.person_id
    having count(distinct am.medication_order_id) >= 3
),

-- Rule 7: Latest BP in last 12 months with diastolic > 90
rule_7_bp_diastolic as (
    select person_id
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date > dateadd(month, -12, current_date())
      and diastolic_value > 90
),

-- Rule 8: Latest BP in last 12 months with systolic > 150
rule_8_bp_systolic as (
    select person_id
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date > dateadd(month, -12, current_date())
      and systolic_value > 150
),

-- Combine rule results for all CKD register patients (excluding PG1)
-- Rules 2-5: independent inclusion criteria (eGFR/ACR thresholds)
-- Rule 6: antihypertensive is a gate (must-match) for Rules 7-8 (BP)
-- Rules 7-8: BP inclusion only applies if Rule 6 (antihypertensive gate) passes
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is not null) as rule_2_egfr_acr_compound,
        (r3.person_id is not null) as rule_3_egfr_acr_compound,
        (r4.person_id is not null) as rule_4_egfr_range,
        (r5.person_id is not null) as rule_5_acr_range,
        (r6.person_id is not null) as rule_6_antihypertensive,
        (r7.person_id is not null) as rule_7_bp_diastolic,
        (r8.person_id is not null) as rule_8_bp_systolic,
        case
            when r2.person_id is not null then 'Included'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is not null then 'Included'
            -- Rule 6 is a gate: must have 3+ antihypertensives to reach BP rules
            when r6.person_id is not null and (r7.person_id is not null or r8.person_id is not null) then 'Included'
            else 'Excluded'
        end as final_status
    from ckd_register cr
    left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
    left join rule_2_combined r2 on cr.person_id = r2.person_id
    left join rule_3_combined r3 on cr.person_id = r3.person_id
    left join rule_4_egfr_range r4 on cr.person_id = r4.person_id
    left join rule_5_acr_range r5 on cr.person_id = r5.person_id
    left join rule_6_antihypertensive r6 on cr.person_id = r6.person_id
    left join rule_7_bp_diastolic r7 on cr.person_id = r7.person_id
    left join rule_8_bp_systolic r8 on cr.person_id = r8.person_id
    where pg1.person_id is null  -- Exclude PG1 patients
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'CKD' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
