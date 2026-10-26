-- LTC LCS: Asthma Adult Register - Priority Group 2 (High Risk)
-- Parent population: Asthma register, age >= 18, excluding PG1 (HRC)
-- EMIS source: 'On Asthma(Adult) Register- LTC LCS Priority Group 2 (HR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/asthma_adult/on_asthma_adult_register_ltc_lcs_priority_group_2_hr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC) patients
-- - Rule 2: emergency asthma admission code (vs1) in last 12 months -> include
-- - Rule 3: tiotropium (vs2) in last 6 months, montelukast (vs3) or
--   theophylline/aminophylline (vs4) in last 12 months -> include
-- - Rule 4: >= 3 prednisolone (vs5) orders in last 12 months -> include
-- - Rule 5: >= 3 antibiotics (vs6: amoxicillin, doxycycline etc.) orders in
--   last 12 months -> include
-- - Rule 6: any high-dose ICS/LABA inhaler (vs7: Fostair 200 etc.) orders in
--   last 6 months -> include
-- - Rule 7: beclometasone/formoterol combination (vs8) and Beclazone-type
--   inhaler (vs9) both in last 6 months -> include, else exclude
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

-- Rule 1: Exclude PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ast_pg1_hrc') }}
),

-- Rule 2: emergency asthma admission code in last 12 months
rule_2_emergency_admission as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_asthma_adult_reg_pg2_hr_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

-- Rule 3: tiotropium in last 6 months, montelukast or theophylline in last 12 months
rule_3_add_on_therapy as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg2_hr_vs2") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg2_hr_vs3, on_asthma_adult_reg_pg2_hr_vs4") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Rule 4: >= 3 prednisolone orders in last 12 months
rule_4_prednisolone as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_asthma_adult_reg_pg2_hr_vs5") }})
    where order_date >= dateadd(month, -12, current_date())
    group by person_id
    having count(distinct medication_order_id) >= 3
),

-- Rule 5: >= 3 antibiotics orders in last 12 months
rule_5_antibiotics as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_asthma_adult_reg_pg2_hr_vs6") }})
    where order_date >= dateadd(month, -12, current_date())
    group by person_id
    having count(distinct medication_order_id) >= 3
),

-- Rule 6 has no issue count in the XML. Exclude products explicitly listed in rule 7
-- until the analyser's SCT_PREP expansion stops absorbing moderate-dose brands.
moderate_dose_products as (
    select distinct original_code, translated_to_snomed_code, display_name
    from {{ ref('stg_reference_ltc_lcs_original_codes') }}
    where valueset_id in ('3c6a4c09-3104-0c3d-d812-9a35e9208512', '7f679be0-8ad9-434f-ad03-e0c29ac330f4')
),
rule_6_high_dose_inhaler as (
    select distinct mo.person_id
    from ({{ get_ltc_lcs_medication_orders("on_asthma_adult_reg_pg2_hr_vs7") }}) as mo
    where mo.order_date >= dateadd(month, -6, current_date())
        and not exists (
            select 1 from moderate_dose_products as moderate
            where moderate.original_code = mo.concept_code
                or moderate.translated_to_snomed_code = mo.concept_code
                or moderate.display_name = mo.concept_display
        )
),

-- Rule 7: beclometasone/formoterol combination and Beclazone-type inhaler
-- both in last 6 months
rule_7_combination_inhalers as (
    select v8.person_id
    from (
        select person_id
        from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg2_hr_vs8") }})
        where order_date >= dateadd(month, -6, current_date())
    ) v8
    inner join (
        select person_id
        from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg2_hr_vs9") }})
        where order_date >= dateadd(month, -6, current_date())
    ) v9 on v8.person_id = v9.person_id
),

-- Combine rule results for all adult asthma register patients
patient_rules as (
    select
        ar.person_id,
        (pg1.person_id is not null) as rule_1_in_pg1,
        (r2.person_id is not null) as rule_2_emergency_admission,
        (r3.person_id is not null) as rule_3_add_on_therapy,
        (r4.person_id is not null) as rule_4_prednisolone,
        (r5.person_id is not null) as rule_5_antibiotics,
        (r6.person_id is not null) as rule_6_high_dose_inhaler,
        (r7.person_id is not null) as rule_7_combination_inhalers,
        case
            when pg1.person_id is not null then 'Excluded'
            when r2.person_id is not null
                or r3.person_id is not null
                or r4.person_id is not null
                or r5.person_id is not null
                or r6.person_id is not null
                or r7.person_id is not null
                then 'Included'
            else 'Excluded'
        end as final_status
    from asthma_adult_register ar
    left join pg1_exclusions pg1 on ar.person_id = pg1.person_id
    left join rule_2_emergency_admission r2 on ar.person_id = r2.person_id
    left join rule_3_add_on_therapy r3 on ar.person_id = r3.person_id
    left join rule_4_prednisolone r4 on ar.person_id = r4.person_id
    left join rule_5_antibiotics r5 on ar.person_id = r5.person_id
    left join rule_6_high_dose_inhaler r6 on ar.person_id = r6.person_id
    left join rule_7_combination_inhalers r7 on ar.person_id = r7.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Asthma Adult' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
