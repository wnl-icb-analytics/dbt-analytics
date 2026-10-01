-- LTC LCS: Stroke/TIA Register - Priority Group 3 (Medium Risk)
-- Parent population: Stroke/TIA register, excluding PG1 (HRC) and PG2 (HR)
-- EMIS source: 'On Stroke/TIA Register- LTC LCS Priority Group 3 (MR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/stroke_tia/on_stroke_tia_register_ltc_lcs_priority_group_3_mr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC) and PG2 (HR) patients
-- - Rule 2 (gate): stroke (vs1) or TIA (vs2) code before 1 year ago. Fail -> exclude.
-- - Rule 3: latest non-HDL cholesterol (vs3) > 2.5 -> include
-- - Rule 4 (gate): high-intensity statin order (vs4) in last 6 months, or current
--   repeat course of any statin (vs5) -> exclude (on optimal therapy)
-- - Rule 5: include if NOT on statins - no statin order (vs6) in last 12 months,
--   no statin clinical code (vs6) in last 12 months and no 'statin declined'
--   code (vs7) in last year. Otherwise exclude.

with
-- Parent population: Patients currently on Stroke/TIA register
stroke_tia_register as (
    select distinct person_id
    from {{ ref('fct_person_stroke_tia_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG1 (HRC) and PG2 (HR)
pg1_pg2_exclusions as (
    select distinct person_id from {{ ref('int_ltc_lcs_rs_stroke_pg1_hrc') }}
    union
    select distinct person_id from {{ ref('int_ltc_lcs_rs_stroke_pg2_hr') }}
),

-- Rule 2: stroke/TIA code before 1 year ago
rule_2_stale_stroke as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_stroketia_reg_pg3_mr_vs1, on_stroketia_reg_pg3_mr_vs2") }})
    where clinical_effective_date < dateadd(year, -1, current_date())
),

-- Rule 3: latest non-HDL cholesterol > 2.5
rule_3_non_hdl_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_stroketia_reg_pg3_mr_vs3") }})
    where result_value > 2.5
),

-- Rule 4: on optimal statin therapy - high-intensity statin order in last
-- 6 months, or a current repeat-type course of any statin
rule_4_high_intensity_statins as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_stroketia_reg_pg3_mr_vs4") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_medication_statements("on_stroketia_reg_pg3_mr_vs5") }})
    where is_active = true
        and authorisation_type_display = 'Repeated prescription'
),

-- Rule 5: any statin therapy - statin order or clinical code (vs6, STAT_COD
-- refset) in last 12 months, or statin declined (vs7) in last year
rule_5_any_statins as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_stroketia_reg_pg3_mr_vs6") }})
    where order_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_stroketia_reg_pg3_mr_vs6") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_stroketia_reg_pg3_mr_vs7") }})
    where clinical_effective_date > dateadd(year, -1, current_date())
),

-- Combine rule results for all Stroke/TIA register patients
patient_rules as (
    select
        sr.person_id,
        (excl.person_id is not null) as rule_1_in_pg1_pg2,
        (r2.person_id is not null) as rule_2_stale_stroke,
        (r3.person_id is not null) as rule_3_non_hdl_high,
        (r4.person_id is not null) as rule_4_high_intensity_statins,
        (r5.person_id is not null) as rule_5_any_statins,
        case
            when excl.person_id is not null then 'Excluded'
            when r2.person_id is null then 'Excluded'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Excluded'
            when r5.person_id is null then 'Included'
            else 'Excluded'
        end as final_status
    from stroke_tia_register sr
    left join pg1_pg2_exclusions excl on sr.person_id = excl.person_id
    left join rule_2_stale_stroke r2 on sr.person_id = r2.person_id
    left join rule_3_non_hdl_high r3 on sr.person_id = r3.person_id
    left join rule_4_high_intensity_statins r4 on sr.person_id = r4.person_id
    left join rule_5_any_statins r5 on sr.person_id = r5.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Stroke/TIA' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
