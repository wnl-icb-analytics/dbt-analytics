/*
Machine-readable source coverage so packs and consumers can annotate asymmetric
footprints, such as EPD lag, instead of discovering them.
*/

{{ config(materialized = 'table') }}

with slam_bounds as (
    select max(activity_month) as slam_max_month
    from {{ ref('fct_person_cost_index_monthly') }}
    where cost_source = 'SLAM'
)

select
    f.cost_source
    , f.cost_basis
    , min(f.activity_month)                          as min_month
    , max(f.activity_month)                          as max_month
    , count(distinct f.activity_month)               as months_present
    , sum(f.total_cost)                              as total_cost
    , max(f.activity_month) >= max(b.slam_max_month) as is_current
from {{ ref('fct_person_cost_index_monthly') }} as f
cross join slam_bounds as b
group by
    f.cost_source
    , f.cost_basis
