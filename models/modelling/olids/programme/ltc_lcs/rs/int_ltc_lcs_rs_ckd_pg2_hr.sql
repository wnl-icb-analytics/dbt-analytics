-- LTC LCS: CKD Register - Priority Group 2 (High Risk)
-- Parent population: CKD register, excluding PG1 (HRC)
--
-- One row per included person. Rules 2-5 select independently; otherwise rule 6
-- (3+ repeat antihypertensive issues strictly within three months) gates rules 7-10.
-- BP uses the shared LTC LCS selector for clinic and home readings.
-- Thresholds are diastolic > 90 and systolic >= 1, or systolic > 150 and
-- diastolic >= 1. XML descriptions mentioning 140/90 or 135/85 are stale.

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

-- XML group 2011000033113 "Antihypertensive Drugs" is BNF section 2.5
-- (legacyValue 2.5, including children), rather than the shared SNOMED list.
rule_6_bnf_products as (
    select distinct p.product_code, p.vpid, p.vtm_id, p.form_code
    from {{ ref('dmd_product') }} as p
    inner join {{ ref('stg_reference_bnf_latest') }} as b on p.product_code = b.snomed_code
    where b.bnf_code like '0205%'
),

-- Missing-BNF products have repeat issues. Backfill only those products, retaining
-- known BNF mappings and complete ingredient combinations. This reconstructs the
-- section; it is not an exported EMIS product list.
rule_6_unmapped_products as (
    select distinct p.product_code, p.vpid, p.vtm_id, p.form_code
    from {{ ref('dmd_product') }} as p
    where not exists (
        select 1 from {{ ref('stg_reference_bnf_latest') }} as b
        where b.snomed_code = p.product_code and b.bnf_code is not null
    )
),

rule_6_ingredients as (
    select
        vpid,
        listagg(ingredient_substance_id, '|') within group (
            order by ingredient_substance_id
        ) as ingredient_set
    -- Match dmd_product: use current ingredients, or retained rows for removed VMPs.
    from (
        select vpid, ingredient_substance_id
        from {{ ref('stg_ukhfd_dmd_vmp_ingredient') }}
        qualify is_in_latest_source
            or count_if(is_in_latest_source) over (partition by vpid) = 0
    )
    group by vpid
),

rule_6_backfill_products as (
    select p.product_code
    from rule_6_unmapped_products as p
    inner join rule_6_bnf_products as b on p.vtm_id = b.vtm_id and p.form_code = b.form_code
    union
    select p.product_code
    from rule_6_unmapped_products as p
    inner join rule_6_ingredients as i on p.vpid = i.vpid
    inner join rule_6_ingredients as j on i.ingredient_set = j.ingredient_set
    inner join rule_6_bnf_products as b on j.vpid = b.vpid and p.form_code = b.form_code
    union
    select p.product_code
    from rule_6_unmapped_products as p
    inner join {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} as t
        on p.product_code = t.sub_type_id::varchar
    inner join rule_6_bnf_products as b on t.super_type_id::varchar = b.product_code
),

rule_6_orders as (
    select medication_order_id, medication_statement_id, person_id, order_date
    from ({{ get_medication_orders(bnf_code='0205') }})
    union all
    select mo.id, mo.medication_statement_id, mo.person_id, mo.clinical_effective_date
    from {{ ref('stg_olids_medication_order') }} as mo
    inner join rule_6_backfill_products as p on mo.mapped_concept_code = p.product_code
    where mo.bnf_code is null
),

rule_6_antihypertensive as (
    select mo.person_id
    from rule_6_orders as mo
    inner join {{ ref('stg_olids_medication_statement') }} as ms
        on mo.medication_statement_id = ms.id
    where mo.order_date > dateadd(month, -3, current_date())
        and ms.authorisation_type_source_code = 'Repeat'
    group by mo.person_id
    having count(distinct mo.medication_order_id) >= 3
),

bp_latest as (
    select *
    from {{ ref('int_ltc_lcs_bp_latest_by_setting') }}
),

rule_7_bp_diastolic as (
    select person_id
    from bp_latest
    where is_latest_reading_clinic
        and latest_reading_date > dateadd(month, -12, current_date())
        and clinic_reading_date > dateadd(month, -12, current_date())
        and clinic_diastolic_value > 90 and clinic_systolic_value >= 1
),

rule_8_bp_systolic as (
    select person_id
    from bp_latest
    where is_latest_reading_clinic
        and latest_reading_date > dateadd(month, -12, current_date())
        and clinic_reading_date > dateadd(month, -12, current_date())
        and clinic_systolic_value > 150 and clinic_diastolic_value >= 1
),

rule_9_home_bp_diastolic as (
    select person_id
    from bp_latest
    where is_latest_reading_home
        and home_reading_date > dateadd(month, -12, current_date())
        and home_diastolic_value > 90 and home_systolic_value >= 1
),

rule_10_home_bp_systolic as (
    select person_id
    from bp_latest
    where is_latest_reading_home
        and home_reading_date > dateadd(month, -12, current_date())
        and home_systolic_value > 150 and home_diastolic_value >= 1
),

-- Combine rule results for all CKD register patients (excluding PG1)
-- Rules 2-5: independent inclusion criteria (eGFR/ACR thresholds)
-- Rule 6: antihypertensive is a gate (must-match) for Rules 7-10 (BP)
-- Rules 7-10: BP inclusion only applies if Rule 6 (antihypertensive gate) passes
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
        (r9.person_id is not null) as rule_9_home_bp_diastolic,
        (r10.person_id is not null) as rule_10_home_bp_systolic,
        case
            when r2.person_id is not null then 'Included'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is not null then 'Included'
            -- Rule 6 is a gate: must have 3+ antihypertensives to reach BP rules
            when r6.person_id is not null and (r7.person_id is not null or r8.person_id is not null
                or r9.person_id is not null or r10.person_id is not null) then 'Included'
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
    left join rule_9_home_bp_diastolic r9 on cr.person_id = r9.person_id
    left join rule_10_home_bp_systolic r10 on cr.person_id = r10.person_id
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
