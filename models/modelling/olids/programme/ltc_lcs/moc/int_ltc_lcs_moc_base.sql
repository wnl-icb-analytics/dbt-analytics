{{ config(materialized='table') }}

-- Register members, including people excluded from the full adult MoC.
-- Registration status is not filtered; consumers filter is_active.
with register_population as (
    select r.person_id, 'af' as condition
    from {{ ref('fct_person_atrial_fibrillation_register') }} r
    union all
    select r.person_id, 'ckd' as condition
    from {{ ref('fct_person_ckd_register') }} r
    union all
    select r.person_id, 'chd' as condition
    from {{ ref('fct_person_chd_register') }} r
    union all
    select r.person_id, 'diabetes' as condition
    from {{ ref('fct_person_diabetes_register') }} r
    union all
    select r.person_id, 'hypertension' as condition
    from {{ ref('fct_person_hypertension_register') }} r
    union all
    select r.person_id, 'nafld' as condition
    from {{ ref('int_ltc_lcs_nafld_register') }} r
    union all
    select r.person_id, 'asthma_adult' as condition
    from {{ ref('fct_person_asthma_register') }} r
    inner join {{ ref('dim_person_age') }} age on r.person_id = age.person_id
    where age.age >= 18
    union all
    select r.person_id, 'asthma_cyp' as condition
    from {{ ref('fct_person_cyp_asthma_register') }} r
    where r.is_on_register
    union all
    select r.person_id, 'copd' as condition
    from {{ ref('int_ltc_lcs_copd_register') }} r
    union all
    select r.person_id, 'hf' as condition
    from {{ ref('fct_person_heart_failure_register') }} r
    union all
    select r.person_id, 'pad' as condition
    from {{ ref('fct_person_pad_register') }} r
    union all
    select r.person_id, 'stroke_tia' as condition
    from {{ ref('fct_person_stroke_tia_register') }} r
),
person_registers as (
    select
        r.person_id,
        count(distinct condition) as number_of_ltc_lcs_conditions,
        max(condition = 'af') as is_on_af_register,
        max(condition = 'ckd') as is_on_ckd_register,
        max(condition = 'chd') as is_on_chd_register,
        max(condition = 'diabetes') as is_on_diabetes_register,
        max(condition = 'hypertension') as is_on_hypertension_register,
        max(condition = 'nafld') as is_on_nafld_register,
        max(condition = 'asthma_adult') as is_on_asthma_adult_register,
        max(condition = 'asthma_cyp') as is_on_asthma_cyp_register,
        max(condition = 'copd') as is_on_copd_register,
        max(condition = 'hf') as is_on_hf_register,
        max(condition = 'pad') as is_on_pad_register,
        max(condition = 'stroke_tia') as is_on_stroke_tia_register
    from register_population r
    group by r.person_id
),
with_exclusion as (
    select
        p.*,
        case
            when p.number_of_ltc_lcs_conditions = 1 and p.is_on_asthma_cyp_register
                then 'CYP asthma only'
            when p.number_of_ltc_lcs_conditions = 1 and p.is_on_hypertension_register
                and htn.person_id is not null then 'LR hypertension only'
            when p.number_of_ltc_lcs_conditions = 1 and p.is_on_asthma_adult_register
                and ast.person_id is not null then 'LR adult asthma only'
        end as moc_base_exclusion_reason
    from person_registers p
    left join {{ ref('int_ltc_lcs_rs_htn_pg4_lr') }} htn on p.person_id = htn.person_id
    left join {{ ref('int_ltc_lcs_rs_ast_pg4_lr') }} ast on p.person_id = ast.person_id
)
select
    *,
    moc_base_exclusion_reason is null as is_in_moc_base,
    coalesce(moc_base_exclusion_reason = 'CYP asthma only', false) as is_cyp_asthma_only,
    -- Programme rule: LR single-condition people need only the annual Check & Test.
    coalesce(moc_base_exclusion_reason in ('LR hypertension only', 'LR adult asthma only'), false)
        as is_moc_check_test_only
from with_exclusion
