{{ config(materialized='table', tags=['cltcs']) }}

-- Care pathway gap items for the C-LTCS population: one row per person and assessable item.
-- NICE indicators come from cltcs_treatment_nice_indicators. A disease control result that
-- is missing is a monitoring gap in the same group, so each item lands in exactly one
-- domain. NOT_ASSESSABLE results are neither a gap nor assessable.

with spine_persons as (
    select distinct person_id
    from {{ ref('cltcs_adult_population') }}
    where meets_original_criteria or meets_adults_with_complexity
),

nice_items as (
    select ns.person_id
        , ns.indicator_id as item_id
        , ns.indicator_name as item_name
        , 'nice' as item_source
        , case
            when m.score_domain = 'disease_control'
                and ns.indicator_status = 'NOT_RECORDED_IN_PERIOD' then 'monitoring_gaps'
            else m.score_domain
          end as score_domain
        , m.score_group
        , ns.indicator_status
        , ns.indicator_status <> 'ACHIEVED' as is_gap
    from {{ ref('fct_person_nice_indicator_status') }} ns
    inner join {{ ref('cltcs_treatment_nice_indicators') }} m
        on ns.indicator_id = m.indicator_id
    inner join spine_persons sp
        on ns.person_id = sp.person_id
    where ns.indicator_status <> 'NOT_ASSESSABLE'
),

-- respiratory disease activity: rescue steroid courses (glucocorticoid order days) in the
-- last 12 months. For COPD a course counts only when an antibacterial was ordered the same
-- day, as a proxy for a treated exacerbation.
respiratory_spine as (
    select sp.person_id
        , pc.has_asthma
        , pc.has_copd
    from spine_persons sp
    inner join {{ ref('dim_person_conditions') }} pc
        on sp.person_id = pc.person_id
    where pc.has_asthma or pc.has_copd
),

steroid_days as (
    select distinct s.person_id
        , s.order_date
    from {{ ref('int_systemic_corticosteroid_medications_all') }} s
    inner join respiratory_spine rs
        on s.person_id = rs.person_id
    where s.is_anti_inflammatory
        and s.order_date > dateadd(month, -12, current_date())
        and s.order_date <= current_date()
),

antibacterial_days as (
    select distinct a.person_id
        , a.order_date
    from {{ ref('int_antibacterial_medications_all') }} a
    inner join respiratory_spine rs
        on a.person_id = rs.person_id
    where a.order_date > dateadd(month, -12, current_date())
        and a.order_date <= current_date()
),

respiratory_activity as (
    select rs.person_id
        , rs.has_asthma
        , rs.has_copd
        , count(sd.order_date) as steroid_course_days_12mo
        , count(ad.order_date) as steroid_with_antibacterial_days_12mo
    from respiratory_spine rs
    left join steroid_days sd
        on rs.person_id = sd.person_id
    left join antibacterial_days ad
        on sd.person_id = ad.person_id
        and sd.order_date = ad.order_date
    group by rs.person_id, rs.has_asthma, rs.has_copd
),

-- diabetes care processes from the project's 9 care processes model (diabetes register,
-- 12 months). HbA1c and blood pressure are left out because the NICE HbA1c and blood
-- pressure indicators already score them, so each missing test counts once.
diabetes_care_processes as (
    select cp.person_id
        , coalesce(cp.cholesterol_completed_in_last_12m, false) as cholesterol_completed_in_last_12m
        , coalesce(cp.creatinine_completed_in_last_12m, false) as creatinine_completed_in_last_12m
        , coalesce(cp.acr_completed_in_last_12m, false) as acr_completed_in_last_12m
        , coalesce(cp.foot_check_completed_in_last_12m, false) as foot_check_completed_in_last_12m
        , coalesce(cp.bmi_completed_in_last_12m, false) as bmi_completed_in_last_12m
        , coalesce(cp.smoking_completed_in_last_12m, false) as smoking_completed_in_last_12m
        , coalesce(cp.retinal_screening_completed_in_last_12m, false) as retinal_screening_completed_in_last_12m
    from {{ ref('fct_person_diabetes_9_care_processes') }} cp
    inner join spine_persons sp
        on cp.person_id = sp.person_id
),

diabetes_care_process_definitions as (
    select process_column, item_id, item_name
    from values
        ('CHOLESTEROL_COMPLETED_IN_LAST_12M', 'DM_CP_CHOLESTEROL', 'Diabetes care process: cholesterol in 12 months'),
        ('CREATININE_COMPLETED_IN_LAST_12M', 'DM_CP_CREATININE', 'Diabetes care process: serum creatinine in 12 months'),
        ('ACR_COMPLETED_IN_LAST_12M', 'DM_CP_ACR', 'Diabetes care process: urine albumin to creatinine ratio in 12 months'),
        ('FOOT_CHECK_COMPLETED_IN_LAST_12M', 'DM_CP_FOOT_CHECK', 'Diabetes care process: foot check in 12 months'),
        ('BMI_COMPLETED_IN_LAST_12M', 'DM_CP_BMI', 'Diabetes care process: BMI in 12 months'),
        ('SMOKING_COMPLETED_IN_LAST_12M', 'DM_CP_SMOKING', 'Diabetes care process: smoking status in 12 months'),
        ('RETINAL_SCREENING_COMPLETED_IN_LAST_12M', 'DM_CP_RETINAL_SCREENING', 'Diabetes care process: retinal screening in 12 months')
        as d (process_column, item_id, item_name)
),

diabetes_care_process_items as (
    select p.person_id
        , d.item_id
        , d.item_name
        , not p.is_completed as is_gap
    from (
        select *
        from diabetes_care_processes
            unpivot (is_completed for process_column in (
                cholesterol_completed_in_last_12m, creatinine_completed_in_last_12m,
                acr_completed_in_last_12m, foot_check_completed_in_last_12m,
                bmi_completed_in_last_12m, smoking_completed_in_last_12m,
                retinal_screening_completed_in_last_12m
            ))
    ) p
    inner join diabetes_care_process_definitions d
        on p.process_column = d.process_column
),

bespoke_items as (
    -- eGFR overdue for its interval (45-59 yearly, 30-44 6-monthly, <30 ~quarterly)
    select eg.person_id
        , 'EGFR_OVERDUE' as item_id
        , 'eGFR overdue for the interval its latest value requires' as item_name
        , 'monitoring_gaps' as score_domain
        , 'ckd' as score_group
        , case
            when eg.egfr_value >= 45
                and eg.clinical_effective_date < dateadd(month, -12, current_date()) then true
            when eg.egfr_value >= 30 and eg.egfr_value < 45
                and eg.clinical_effective_date < dateadd(month, -6, current_date()) then true
            when eg.egfr_value < 30
                and eg.clinical_effective_date < dateadd(month, -3, current_date()) then true
            else false
          end as is_gap
    from {{ ref('int_egfr_latest') }} eg
    inner join spine_persons sp
        on eg.person_id = sp.person_id
    where eg.egfr_value >= 0 and eg.egfr_value < 60
    union all
    -- diabetes care processes not done (one item per process)
    select person_id
        , item_id
        , item_name
        , 'monitoring_gaps'
        , 'diabetes'
        , is_gap
    from diabetes_care_process_items
    union all
    -- very high HbA1c: latest HbA1c in 12 months above 90 mmol/mol (IFCC) for people on the
    -- diabetes register. Sits in the diabetes control group with IND179/180, so it adds depth
    -- to an above-target result rather than counting as another condition.
    select dcp.person_id
        , 'HBA1C_ABOVE_90'
        , 'Diabetes: latest HbA1c in 12 months above 90 mmol/mol'
        , 'disease_control'
        , 'diabetes'
        , hb.hba1c_ifcc > 90
    from diabetes_care_processes dcp
    inner join {{ ref('int_hba1c_latest') }} hb
        on dcp.person_id = hb.person_id
    where hb.hba1c_ifcc is not null
        and hb.clinical_effective_date > dateadd(month, -12, current_date())
        and hb.clinical_effective_date <= current_date()
    union all
    -- asthma treated with salbutamol and no other asthma inhaler (no preventer)
    select sp.person_id
        , 'ASTHMA_SALBUTAMOL_ONLY'
        , 'Asthma treated with salbutamol and no other asthma inhaler'
        , 'treatment_optimisation'
        , 'asthma'
        , coalesce(am.salbutamol_only, false)
    from spine_persons sp
    inner join {{ ref('dim_person_conditions') }} pc
        on sp.person_id = pc.person_id
    left join {{ ref('int_asthma_management') }} am
        on sp.person_id = am.person_id
    where pc.has_asthma
    union all
    -- uncontrolled respiratory disease: one item per trigger, all in the respiratory group
    select person_id
        , 'COPD_EXACERBATIONS'
        , 'COPD: 2 or more glucocorticoid order days with a same-day antibacterial in 12 months'
        , 'disease_control'
        , 'respiratory'
        , steroid_with_antibacterial_days_12mo >= 2
    from respiratory_activity
    where has_copd
    union all
    select person_id
        , 'ASTHMA_STEROID_COURSES'
        , 'Asthma: 2 or more glucocorticoid order days in 12 months'
        , 'disease_control'
        , 'respiratory'
        , steroid_course_days_12mo >= 2
    from respiratory_activity
    where has_asthma
    union all
    select ra.person_id
        , 'ASTHMA_SALBUTAMOL_OVERUSE'
        , 'Asthma: 3 or more salbutamol inhalers in 12 months'
        , 'disease_control'
        , 'respiratory'
        , coalesce(am.salbutamol_repeats, false)
    from respiratory_activity ra
    left join {{ ref('int_asthma_management') }} am
        on ra.person_id = am.person_id
    where ra.has_asthma
)

select person_id
    , item_id
    , item_name
    , item_source
    , score_domain
    , score_group
    , indicator_status
    , is_gap
from nice_items
union all
select person_id
    , item_id
    , item_name
    , 'bespoke'
    , score_domain
    , score_group
    , null
    , is_gap
from bespoke_items
