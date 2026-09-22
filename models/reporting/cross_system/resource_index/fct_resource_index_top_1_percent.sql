/*
Costliest 1% compared with the other 99% for supplementary resource-to-need
tables.

Grain: breakdown dimension x category. The cohort is assigned once in
fct_person_resource_index_supplementary. Both patient and cost shares are exposed because
the workbook uses both concepts. LTC rows use NCL OLIDS-covered patients only.
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
        sum(mhsds_proxy_cost_12m) as mhsds_proxy_cost_12m,
        sum(csds_proxy_cost_12m) as csds_proxy_cost_12m,
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
        div0(a.total_cost_12m, a.weighted_person_years) as cost_per_weighted_head
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
        max(iff(cost_cohort = 'Top 1%', mhsds_proxy_cost_12m, null)) as top_1_mhsds_proxy_cost,
        max(iff(cost_cohort = 'Other 99%', mhsds_proxy_cost_12m, null)) as other_99_mhsds_proxy_cost,
        max(iff(cost_cohort = 'Top 1%', csds_proxy_cost_12m, null)) as top_1_csds_proxy_cost,
        max(iff(cost_cohort = 'Other 99%', csds_proxy_cost_12m, null)) as other_99_csds_proxy_cost,
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
