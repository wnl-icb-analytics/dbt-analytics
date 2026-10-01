-- LTC LCS: PAD Register - Priority Group 3 (Medium Risk)
-- Parent population: PAD register, excluding PG1 (HRC) and PG2 (HR)
-- EMIS source: 'On PAD Register- LTC LCS Priority Group 3 (MR)'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/pad/on_pad_register_ltc_lcs_priority_group_3_mr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC) and PG2 (HR) patients
-- - Rule 2 (gate): peripheral ischaemia code (vs1) before 12 months ago. Fail -> exclude.
-- - Rule 3: latest non-HDL cholesterol (vs2) > 2.5 -> include
-- - Rule 4 (gate): high-intensity statin order (vs3) in last 6 months, or current
--   repeat course of any statin (vs4) -> exclude (on optimal therapy)
-- - Rule 5: include if NOT on statins - no statin order (vs5) in last 12 months,
--   no statin clinical code (vs5) in last 12 months and no 'statin declined'
--   code (vs6) in last year. Otherwise exclude.

with
-- Parent population: Patients currently on PAD register
pad_register as (
    select distinct person_id
    from {{ ref('fct_person_pad_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG1 (HRC) and PG2 (HR)
pg1_pg2_exclusions as (
    select distinct person_id from {{ ref('int_ltc_lcs_rs_pad_pg1_hrc') }}
    union
    select distinct person_id from {{ ref('int_ltc_lcs_rs_pad_pg2_hr') }}
),

-- Rule 2: peripheral ischaemia code before 12 months ago
rule_2_stale_ischaemia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_pad_reg_pg3_mr_vs1") }})
    where clinical_effective_date < dateadd(month, -12, current_date())
),

-- Rule 3: latest non-HDL cholesterol > 2.5
rule_3_non_hdl_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_pad_reg_pg3_mr_vs2") }})
    where result_value > 2.5
),

-- Rule 4: on optimal statin therapy - high-intensity statin order in last
-- 6 months, or a current repeat-type course of any statin
rule_4_high_intensity_statins as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_pad_reg_pg3_mr_vs3") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_medication_statements("on_pad_reg_pg3_mr_vs4") }})
    where is_active = true
        and authorisation_type_display = 'Repeated prescription'
),

-- Rule 5: any statin therapy - statin order or clinical code (vs5, STAT_COD
-- refset) in last 12 months, or statin declined (vs6) in last year
rule_5_any_statins as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_pad_reg_pg3_mr_vs5") }})
    where order_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_pad_reg_pg3_mr_vs5") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_pad_reg_pg3_mr_vs6") }})
    where clinical_effective_date > dateadd(year, -1, current_date())
),

-- Combine rule results for all PAD register patients
patient_rules as (
    select
        pr.person_id,
        (excl.person_id is not null) as rule_1_in_pg1_pg2,
        (r2.person_id is not null) as rule_2_stale_ischaemia,
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
    from pad_register pr
    left join pg1_pg2_exclusions excl on pr.person_id = excl.person_id
    left join rule_2_stale_ischaemia r2 on pr.person_id = r2.person_id
    left join rule_3_non_hdl_high r3 on pr.person_id = r3.person_id
    left join rule_4_high_intensity_statins r4 on pr.person_id = r4.person_id
    left join rule_5_any_statins r5 on pr.person_id = r5.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'PAD' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
