/*
Resource index at person level — indirect age-sex standardisation on a
person-time basis.

Population: everyone with WNL registration exposure in the latest rolling
12-month cost window — including the deceased and the deducted, whose spend
the old current-registration filter
dropped (~17% of in-patch patient-keyed spend). One row per patient.

  * actual_cost_12m   - patient-attributable spend over the window
    (rolled from int_person_cost_index_actual_monthly)
  * expected_cost_12m - WNL age-sex cost-per-month rate for the patient's
    band x their months registered
  * weighted_months_12m - Core need basis: the practice's UKHFD Core
    Services weighted/registered ratio (practice_weighted_population) x
    months registered. sum(weighted_months_12m)/12 = weighted person-years, the
    denominator for spend-per-weighted-patient in any cut.

Two index bases:
  age-sex:   resource_index = sum(actual) / sum(expected) — own-data curve.
  Core need: spend per weighted person-year vs the WNL mean. The practice
    denominator is NHS England's Core Services weighted population, which
    excludes the health-inequalities component. The latest allocation base
    year not later than the cost window is used. The practice ratio is spread
    uniformly across its patients; practices with no ratio get the WNL
    exposure-weighted mean (flagged).

Index > 1 = spend above expectation on either basis. Deprivation / morbidity
are exposed as cut dimensions (residence_imd_decile + quintile), not folded
into the weights.

Registered geography attributes via the *window* practice (last registered
month), so leavers and decedents attribute to where they were registered.
Age at window end (at death for decedents); missing age/gender carry an
'Unknown' band matching the curve so their spend still standardises.
*/

{{ config(materialized = 'table') }}

with bounds as (
    select
        max(activity_month)                       as window_end_month,
        dateadd(month, -11, max(activity_month))  as window_start_month
    from {{ ref('int_cost_index_slam_activity_monthly') }}
),

exposure_months as (
    select
        e.sk_patient_id,
        e.activity_month,
        e.practice_code,
        e.date_of_death,
        e.died_in_month
    from {{ ref('int_person_resource_index_exposure_monthly') }} as e
    cross join bounds as b
    where e.activity_month between b.window_start_month and b.window_end_month
),

population_base as (
    select
        e.sk_patient_id,
        count(*)                             as months_registered,
        min(e.activity_month)                as first_month_registered,
        max(e.activity_month)                as last_month_registered,
        max_by(e.practice_code, e.activity_month) as practice_code,
        any_value(e.date_of_death)           as date_of_death,
        boolor_agg(e.died_in_month)          as died_in_window,
        any_value(b.window_end_month)        as window_end_month
    from exposure_months as e
    cross join bounds as b
    group by e.sk_patient_id
),

population as (
    select
        e.sk_patient_id,
        e.months_registered,
        e.first_month_registered,
        e.last_month_registered,
        e.died_in_window,
        e.date_of_death,
        e.practice_code,
        coalesce(p.gender, 'Unknown') as gender,
        p.ethnicity,
        p.residence_imd_decile,
        p.residence_borough,
        p.residence_ward_2025_name,
        p.residence_lsoa_2021_code,
        {{ calculate_age_attributes(
            'p.date_of_birth',
            'e.window_end_month',
            is_deceased_field='e.date_of_death is not null',
            death_date_field='e.date_of_death'
        ) }}
    from population_base as e
    left join {{ ref('dim_person_demographics_basic') }} as p
        on e.sk_patient_id = p.sk_patient_id
),

cost as (
    select
        c.sk_patient_id,
        sum(c.actual_cost) as actual_cost_12m
    from {{ ref('int_person_cost_index_actual_monthly') }} as c
    cross join bounds as b
    where c.activity_month between b.window_start_month and b.window_end_month
    group by 1
),

practice_weights as (
    select
        w.practice_code,
        w.financial_year,
        w.financial_year_start,
        w.weighted_to_registered_ratio
    from {{ ref('practice_weighted_population') }} as w
    cross join bounds as b
    where w.financial_year_start <= b.window_end_month
    qualify row_number() over (
        partition by w.practice_code
        order by w.financial_year_start desc
    ) = 1
),

-- WNL exposure-weighted mean ratio: fallback for practices with no
-- published weighted list (new/merged codes)
mean_ratio as (
    select
        sum(w.weighted_to_registered_ratio * pop.months_registered)
            / sum(pop.months_registered) as ratio
    from population as pop
    inner join practice_weights as w
        on pop.practice_code = w.practice_code
),

age_sex_curve as (
    select
        coalesce(pop.age_band_nhs, 'Unknown') as age_band,
        pop.gender,
        div0(sum(coalesce(cost.actual_cost_12m, 0)), sum(pop.months_registered))
            as expected_cost_per_month
    from population as pop
    left join cost
        on pop.sk_patient_id = cost.sk_patient_id
    group by 1, 2
)

select
    pop.sk_patient_id,
    pop.gender,
    pop.age,
    coalesce(pop.age_band_nhs, 'Unknown') as age_band,
    pop.ethnicity,
    pop.months_registered,
    pop.first_month_registered,
    pop.last_month_registered,
    pop.died_in_window,
    pop.practice_code,
    coalesce(org_bor.borough_registered, 'Unknown') as registered_borough,
    coalesce(nb_reg.neighbourhood_name, 'Unknown')  as registered_neighbourhood_name,
    org_bor.sub_icb_code                            as registered_sub_icb_code,
    org_bor.sub_icb_name                            as registered_sub_icb_name,
    pop.residence_imd_decile,
    ceil(pop.residence_imd_decile / 2)              as residence_imd_quintile,
    pop.residence_borough,
    pop.residence_ward_2025_name,
    pop.residence_lsoa_2021_code,
    coalesce(cost.actual_cost_12m, 0)                       as actual_cost_12m,
    curve.expected_cost_per_month * pop.months_registered   as expected_cost_12m,
    coalesce(w.weighted_to_registered_ratio, mr.ratio)      as practice_weighted_ratio,
    w.financial_year                                       as practice_weight_financial_year,
    w.financial_year_start                                 as practice_weight_financial_year_start,
    w.weighted_to_registered_ratio is null                  as weighted_ratio_imputed,
    coalesce(w.weighted_to_registered_ratio, mr.ratio)
        * pop.months_registered                             as weighted_months_12m
from population as pop
cross join mean_ratio as mr
left join {{ ref('int_organisation_borough_mapping') }} as org_bor
    on pop.practice_code = org_bor.practice_code
left join {{ ref('stg_reference_wnl_gp_practice_neighbourhood') }} as nb_reg
    on pop.practice_code = nb_reg.practice_code
left join practice_weights as w
    on pop.practice_code = w.practice_code
left join cost
    on pop.sk_patient_id = cost.sk_patient_id
left join age_sex_curve as curve
    on coalesce(pop.age_band_nhs, 'Unknown') = curve.age_band
    and pop.gender = curve.gender
