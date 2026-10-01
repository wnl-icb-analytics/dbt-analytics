-- Resource index summarised by registered geography (sub-ICB x borough x
-- neighbourhood) for the WNL exposure population. Convenience aggregate for
-- the Aligning Resource Index pack and dashboards: maps and borough tables.
-- Roll up to borough / sub-ICB by summing the additive columns (never average
-- an index). Per-head measures use registered person-years, not raw
-- headcount, since exposure varies (deaths, deductions).
-- Two index bases: resource_index_age_sex (age-sex, actual/expected) and
-- resource_index_weighted (Carr-Hill, spend per weighted person-year vs the
-- WNL mean).
{{ config(materialized = 'table') }}

with base as (
    select
        registered_sub_icb_code,
        registered_sub_icb_name,
        registered_borough,
        registered_neighbourhood_name,
        count(*)                              as population,
        sum(months_registered) / 12           as person_years,
        sum(weighted_months_12m) / 12         as weighted_person_years,
        count_if(died_in_window)              as died_in_window,
        sum(actual_cost_12m)                  as actual_cost_12m,
        sum(expected_cost_12m)                as expected_cost_12m
    from {{ ref('fct_person_resource_index') }}
    group by 1, 2, 3, 4
),

overall as (
    select sum(actual_cost_12m) / sum(weighted_person_years) as mean_cost_per_weighted_year
    from base
)

select
    b.registered_sub_icb_code,
    b.registered_sub_icb_name,
    b.registered_borough,
    b.registered_neighbourhood_name,
    b.population,
    b.person_years,
    b.weighted_person_years,
    b.died_in_window,
    b.actual_cost_12m,
    b.expected_cost_12m,
    div0(b.actual_cost_12m, b.expected_cost_12m)      as resource_index_age_sex,
    div0(b.actual_cost_12m, b.person_years)           as cost_per_person_year,
    div0(b.expected_cost_12m, b.person_years)         as expected_per_person_year,
    div0(b.actual_cost_12m, b.weighted_person_years)  as spend_per_weighted_year,
    div0(b.actual_cost_12m, b.weighted_person_years)
        / o.mean_cost_per_weighted_year               as resource_index_weighted
from base as b
cross join overall as o
