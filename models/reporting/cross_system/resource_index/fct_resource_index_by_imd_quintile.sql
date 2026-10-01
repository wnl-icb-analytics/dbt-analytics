-- Resource index by residence IMD quintile x residence borough. Powers the
-- deprivation section of the Aligning Resource Index pack, in particular
-- most (Q1) vs least (Q5) deprived actual spend. Null quintile is kept as
-- 'Unknown' so spend reconciles to the WNL total. All numerators and
-- denominators are additive - roll up across boroughs freely.
{{ config(materialized = 'table') }}

with base as (
    select
        coalesce(residence_imd_quintile::varchar, 'Unknown') as residence_imd_quintile,
        coalesce(residence_borough, 'Unknown')               as residence_borough,
        count(*)                      as population,
        sum(months_registered) / 12   as person_years,
        sum(weighted_months_12m) / 12 as weighted_person_years,
        count_if(died_in_window)      as died_in_window,
        sum(actual_cost_12m)          as actual_cost_12m,
        sum(expected_cost_12m)        as expected_cost_12m
    from {{ ref('fct_person_resource_index') }}
    group by 1, 2
),

overall as (
    select sum(actual_cost_12m) / sum(weighted_person_years) as mean_cost_per_weighted_year
    from base
)

select
    b.residence_imd_quintile,
    b.residence_borough,
    b.population,
    b.person_years,
    b.weighted_person_years,
    b.died_in_window,
    b.actual_cost_12m,
    b.expected_cost_12m,
    div0(b.actual_cost_12m, b.expected_cost_12m)      as resource_index_age_sex,
    div0(b.actual_cost_12m, b.person_years)           as cost_per_person_year,
    div0(b.actual_cost_12m, b.weighted_person_years)  as spend_per_weighted_year,
    div0(b.actual_cost_12m, b.weighted_person_years)
        / o.mean_cost_per_weighted_year               as resource_index_weighted
from base as b
cross join overall as o
