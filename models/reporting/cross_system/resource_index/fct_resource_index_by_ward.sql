-- Resource index by residence ward (2025) for the WNL exposure population.
-- Drives the ward choropleths in the Aligning Resource Index pack.
-- Two index bases ("weighted" unqualified = Carr-Hill):
--   age-sex:   resource_index_age_sex = actual / expected (own-data curve);
--              age_sex_weighted_person_years = expected / WNL mean rate
--   Carr-Hill: weighted_person_years = sum(weighted_months_12m)/12;
--              resource_index_weighted = spend per weighted person-year
--              relative to the WNL mean
-- Denominators are WNL-registered person-time by residence ward, NOT ONS ward
-- populations - boundary wards with many non-WNL-registered residents are
-- partial.
{{ config(materialized = 'table') }}

with base as (
    select
        residence_borough,
        residence_ward_2025_name as ward,
        count(*)                     as population,
        sum(months_registered) / 12  as person_years,
        sum(weighted_months_12m) / 12 as weighted_person_years,
        sum(actual_cost_12m)         as actual_cost_12m,
        sum(expected_cost_12m)       as expected_cost_12m
    from {{ ref('fct_person_resource_index') }}
    where residence_ward_2025_name is not null
    group by 1, 2
),

-- Reference means from the full exposure population (including unknown-ward
-- residents), so index = 1.0 means the same spend level as in the area /
-- borough / IMD facts.
overall as (
    select
        sum(actual_cost_12m) / (sum(months_registered) / 12)   as mean_cost_per_person_year,
        sum(actual_cost_12m) / (sum(weighted_months_12m) / 12) as mean_cost_per_weighted_year
    from {{ ref('fct_person_resource_index') }}
)

select
    b.residence_borough,
    b.ward,
    b.population,
    b.person_years,
    b.weighted_person_years,
    b.actual_cost_12m,
    b.expected_cost_12m,
    div0(b.actual_cost_12m, b.person_years)                 as cost_per_person_year,
    -- age-sex basis
    div0(b.expected_cost_12m, o.mean_cost_per_person_year)  as age_sex_weighted_person_years,
    div0(b.actual_cost_12m, b.expected_cost_12m)            as resource_index_age_sex,
    o.mean_cost_per_person_year
        * div0(b.actual_cost_12m, b.expected_cost_12m)      as spend_per_age_sex_weighted_year,
    -- Carr-Hill basis
    div0(b.actual_cost_12m, b.weighted_person_years)        as spend_per_weighted_year,
    div0(b.actual_cost_12m, b.weighted_person_years)
        / o.mean_cost_per_weighted_year                     as resource_index_weighted
from base as b
cross join overall as o
