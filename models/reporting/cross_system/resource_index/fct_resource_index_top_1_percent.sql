/*
Costliest 1% compared with the other 99% for supplementary resource-to-need
tables.

Grain: breakdown dimension x category. The cohort is assigned once in
fct_person_resource_index_supplementary. Both patient and cost shares are exposed because
the workbook uses both concepts. LTC rows use NCL OLIDS-covered patients only.

The service split reports the warehouse service groupings (Crisis, Planned,
Community, Mental Health, Unmapped), which sum to each cohort's total cost.
EPD prescribing sits in Community.
The *_cost_share columns give each line's share of its own cohort-category
cost, so the Top 1% and Other 99% service mixes compare directly. The pack
folds Mental Health into Crisis for its four reporting lines; that is a
presentation choice and is not applied here.
*/

{{ config(materialized = 'table') }}

with categories as (
    select
        p.*,
        f.key::varchar as dimension,
        f.value::varchar as category
    from {{ ref('fct_person_resource_index_supplementary') }} as p,
    lateral flatten(input => object_construct_keep_null(
        'Overall', 'Overall',
        'Gender', p.gender,
        'Ethnicity', p.ethnicity_group,
        'Residence borough', case
            when p.residence_borough in (
                'Barnet', 'Brent', 'Camden', 'Ealing', 'Enfield',
                'Hammersmith and Fulham', 'Haringey', 'Harrow', 'Hillingdon',
                'Hounslow', 'Islington', 'Kensington and Chelsea', 'Westminster'
            ) then p.residence_borough
            else 'Outside WNL / unknown'
        end,
        'Deprivation', p.factor_deprivation_group,
        'Emergency care use', iff(
            p.had_emergency_care,
            'Emergency care use',
            'No emergency care use'
        ),
        'Long-term conditions', iff(
            p.has_one_or_more_ltcs,
            'At least 1 LTC',
            'No recorded LTC'
        )
    )) as f
    where f.key::varchar != 'Long-term conditions'
       or p.has_olids_record
),

aggregated as (
    select
        case dimension
            when 'Overall' then 1
            when 'Gender' then 2
            when 'Ethnicity' then 3
            when 'Residence borough' then 4
            when 'Deprivation' then 5
            when 'Emergency care use' then 6
            when 'Long-term conditions' then 7
        end as dimension_order,
        dimension,
        category,
        cost_cohort,
        count(*) as patients,
        count(age) as patients_with_age,
        avg(age) as mean_age,
        sum(person_years) as person_years,
        sum(weighted_person_years) as weighted_person_years,
        sum(total_cost_12m) as total_cost_12m,
        sum(slam_cost_12m) as slam_cost_12m,
        sum(epd_cost_12m) as epd_cost_12m,
        sum(mhsds_cost_12m) as mhsds_cost_12m,
        sum(csds_cost_12m) as csds_cost_12m,
        sum(crisis_cost_12m) as crisis_cost_12m,
        sum(planned_cost_12m) as planned_cost_12m,
        sum(community_cost_12m) as community_cost_12m,
        sum(mental_health_cost_12m) as mental_health_cost_12m,
        sum(unmapped_cost_12m) as unmapped_cost_12m,
        any_value(window_start_month) as window_start_month,
        any_value(window_end_month) as window_end_month,
        any_value(cost_scope) as cost_scope
    from categories
    group by dimension, category, cost_cohort
),

rates as (
    select
        a.*,
        div0(
            a.patients,
            sum(a.patients) over (partition by a.cost_cohort, a.dimension)
        ) as patient_share,
        div0(
            a.total_cost_12m,
            sum(a.total_cost_12m) over (partition by a.cost_cohort, a.dimension)
        ) as cost_share,
        div0(a.total_cost_12m, a.person_years) as cost_per_head,
        div0(a.total_cost_12m, a.weighted_person_years) as cost_per_weighted_head,
        -- Service mix: each line's share of this cohort-category's own cost.
        div0(a.crisis_cost_12m, a.total_cost_12m) as crisis_cost_share,
        div0(a.planned_cost_12m, a.total_cost_12m) as planned_cost_share,
        div0(a.community_cost_12m, a.total_cost_12m) as community_cost_share,
        div0(a.mental_health_cost_12m, a.total_cost_12m) as mental_health_cost_share,
        div0(a.unmapped_cost_12m, a.total_cost_12m) as unmapped_cost_share
    from aggregated as a
),

benchmark as (
    select
        div0(sum(total_cost_12m), sum(weighted_person_years))
            as wnl_cost_per_weighted_head
    from {{ ref('fct_person_resource_index_supplementary') }}
),

pivoted as (
    select
        dimension_order,
        dimension,
        category,
        max(iff(cost_cohort = 'Top 1%', patients, null)) as top_1_patients,
        max(iff(cost_cohort = 'Other 99%', patients, null)) as other_99_patients,
        max(iff(cost_cohort = 'Top 1%', patient_share, null)) as top_1_patient_share,
        max(iff(cost_cohort = 'Other 99%', patient_share, null)) as other_99_patient_share,
        max(iff(cost_cohort = 'Top 1%', cost_share, null)) as top_1_cost_share,
        max(iff(cost_cohort = 'Other 99%', cost_share, null)) as other_99_cost_share,
        max(iff(cost_cohort = 'Top 1%', mean_age, null)) as top_1_mean_age,
        max(iff(cost_cohort = 'Other 99%', mean_age, null)) as other_99_mean_age,
        max(iff(cost_cohort = 'Top 1%', cost_per_head, null)) as top_1_cost_per_head,
        max(iff(cost_cohort = 'Other 99%', cost_per_head, null)) as other_99_cost_per_head,
        max(iff(
            cost_cohort = 'Top 1%',
            cost_per_weighted_head,
            null
        )) as top_1_cost_per_weighted_head,
        max(iff(
            cost_cohort = 'Other 99%',
            cost_per_weighted_head,
            null
        )) as other_99_cost_per_weighted_head,
        max(iff(cost_cohort = 'Top 1%', total_cost_12m, null)) as top_1_total_cost,
        max(iff(cost_cohort = 'Other 99%', total_cost_12m, null)) as other_99_total_cost,
        max(iff(cost_cohort = 'Top 1%', slam_cost_12m, null)) as top_1_slam_cost,
        max(iff(cost_cohort = 'Other 99%', slam_cost_12m, null)) as other_99_slam_cost,
        max(iff(cost_cohort = 'Top 1%', epd_cost_12m, null)) as top_1_epd_cost,
        max(iff(cost_cohort = 'Other 99%', epd_cost_12m, null)) as other_99_epd_cost,
        max(iff(cost_cohort = 'Top 1%', mhsds_cost_12m, null)) as top_1_mhsds_cost,
        max(iff(cost_cohort = 'Other 99%', mhsds_cost_12m, null)) as other_99_mhsds_cost,
        max(iff(cost_cohort = 'Top 1%', csds_cost_12m, null)) as top_1_csds_cost,
        max(iff(cost_cohort = 'Other 99%', csds_cost_12m, null)) as other_99_csds_cost,
        max(iff(cost_cohort = 'Top 1%', crisis_cost_12m, null)) as top_1_crisis_cost,
        max(iff(cost_cohort = 'Other 99%', crisis_cost_12m, null)) as other_99_crisis_cost,
        max(iff(cost_cohort = 'Top 1%', planned_cost_12m, null)) as top_1_planned_cost,
        max(iff(cost_cohort = 'Other 99%', planned_cost_12m, null)) as other_99_planned_cost,
        max(iff(cost_cohort = 'Top 1%', community_cost_12m, null)) as top_1_community_cost,
        max(iff(cost_cohort = 'Other 99%', community_cost_12m, null)) as other_99_community_cost,
        max(iff(
            cost_cohort = 'Top 1%',
            mental_health_cost_12m,
            null
        )) as top_1_mental_health_cost,
        max(iff(
            cost_cohort = 'Other 99%',
            mental_health_cost_12m,
            null
        )) as other_99_mental_health_cost,
        max(iff(cost_cohort = 'Top 1%', unmapped_cost_12m, null)) as top_1_unmapped_cost,
        max(iff(cost_cohort = 'Other 99%', unmapped_cost_12m, null)) as other_99_unmapped_cost,
        max(iff(cost_cohort = 'Top 1%', crisis_cost_share, null)) as top_1_crisis_cost_share,
        max(iff(
            cost_cohort = 'Other 99%',
            crisis_cost_share,
            null
        )) as other_99_crisis_cost_share,
        max(iff(cost_cohort = 'Top 1%', planned_cost_share, null)) as top_1_planned_cost_share,
        max(iff(
            cost_cohort = 'Other 99%',
            planned_cost_share,
            null
        )) as other_99_planned_cost_share,
        max(iff(
            cost_cohort = 'Top 1%',
            community_cost_share,
            null
        )) as top_1_community_cost_share,
        max(iff(
            cost_cohort = 'Other 99%',
            community_cost_share,
            null
        )) as other_99_community_cost_share,
        max(iff(
            cost_cohort = 'Top 1%',
            mental_health_cost_share,
            null
        )) as top_1_mental_health_cost_share,
        max(iff(
            cost_cohort = 'Other 99%',
            mental_health_cost_share,
            null
        )) as other_99_mental_health_cost_share,
        max(iff(cost_cohort = 'Top 1%', unmapped_cost_share, null)) as top_1_unmapped_cost_share,
        max(iff(
            cost_cohort = 'Other 99%',
            unmapped_cost_share,
            null
        )) as other_99_unmapped_cost_share,
        any_value(window_start_month) as window_start_month,
        any_value(window_end_month) as window_end_month,
        any_value(cost_scope) as cost_scope
    from rates
    group by dimension_order, dimension, category
)

select
    p.*,
    div0(p.top_1_patients, p.top_1_patients + p.other_99_patients)
        as top_1_share_of_category_patients,
    div0(p.top_1_total_cost, p.top_1_total_cost + p.other_99_total_cost)
        as top_1_share_of_category_cost,
    p.top_1_patient_share - p.other_99_patient_share as patient_share_difference,
    p.top_1_cost_share - p.other_99_cost_share as cost_share_difference,
    p.top_1_mean_age - p.other_99_mean_age as mean_age_difference,
    div0(p.top_1_cost_per_weighted_head, b.wnl_cost_per_weighted_head)
        as top_1_resource_index,
    div0(p.other_99_cost_per_weighted_head, b.wnl_cost_per_weighted_head)
        as other_99_resource_index,
    div0(p.top_1_cost_per_weighted_head, b.wnl_cost_per_weighted_head)
        - div0(p.other_99_cost_per_weighted_head, b.wnl_cost_per_weighted_head)
        as resource_index_difference,
    iff(
        p.dimension = 'Long-term conditions',
        'NCL OLIDS-covered patients only',
        null
    ) as denominator_note
from pivoted as p
cross join benchmark as b
