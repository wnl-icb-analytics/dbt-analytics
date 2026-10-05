{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'emis', 'olids']
    )
}}

/*
EMIS/OLIDS Practice Registration Comparison

Compares OLIDS Regular registration counts against EMIS list size to identify discrepancies.
Uses EMIS acceptance criteria for validation.

Acceptance Criteria:
- Aggregate: <1% variance across all practices
- Per-practice: <2% variance OR fewer than 5 persons difference (whichever is greater)

Categorization:
- Meets Criteria: <2% variance OR <5 persons difference
- 2-5% Variance: Between 2-5% variance
- 5-20% Variance: Between 5-20% variance
- 20%+ Variance: 20% or greater variance
- Missing Data: No EMIS or OLIDS data available

Data Sources:
- EMIS: Static extract (date from seed file)
- OLIDS: Regular registrations open on the EMIS extract date, as recorded by the
  daily registration snapshot when it covers that date, otherwise rebuilt from
  current registration history
*/

with emis_registrations as (
    select
        practice_code,
        practice_name,
        borough,
        list_size as emis_list_size,
        extract_date
    from {{ ref('stg_emis_list_size') }}
),

olids_regular_counts as (
    select
        practice_ods_code as practice_code,
        regular_registered_patients as olids_regular_count,
        olids_count_basis,
        snapshot_date
    from {{ ref('int_olids_regular_registrations_at_point_in_time') }}
),

practices as (
    -- Canonical practice name/borough from dim_practice. EMIS seed values are
    -- kept as a fallback for practices that have merged or closed and no longer
    -- appear in the OLIDS organisation feed (e.g. Roman Way → Islington Central).
    select
        practice_code,
        practice_name,
        borough_registered as borough
    from {{ ref('dim_practice') }}
),

comparison as (
    select
        coalesce(e.practice_code, o.practice_code) as practice_code,
        coalesce(p.practice_name, e.practice_name) as practice_name,
        coalesce(p.borough, e.borough) as borough,
        coalesce(e.emis_list_size, 0) as emis_list_size,
        coalesce(o.olids_regular_count, 0) as olids_regular_count,
        o.olids_count_basis,
        e.extract_date,

        -- Calculate difference (signed)
        coalesce(o.olids_regular_count, 0) - coalesce(e.emis_list_size, 0) as difference,

        -- Calculate absolute difference
        abs(coalesce(o.olids_regular_count, 0) - coalesce(e.emis_list_size, 0)) as absolute_difference,

        -- Calculate percentage difference (signed)
        case
            when e.emis_list_size = 0 or e.emis_list_size is null then null
            else round(
                (coalesce(o.olids_regular_count, 0) - e.emis_list_size) * 100.0 / e.emis_list_size,
                2
            )
        end as percent_difference,

        -- Calculate absolute percentage difference
        case
            when e.emis_list_size = 0 or e.emis_list_size is null then null
            else abs(round(
                (coalesce(o.olids_regular_count, 0) - e.emis_list_size) * 100.0 / e.emis_list_size,
                2
            ))
        end as absolute_percent_difference

    from emis_registrations as e
    full outer join olids_regular_counts as o
        on e.practice_code = o.practice_code
    left join practices as p
        on coalesce(e.practice_code, o.practice_code) = p.practice_code
)

select
    practice_code,
    practice_name,
    borough,
    emis_list_size,
    olids_regular_count,
    difference,
    absolute_difference,
    percent_difference,
    absolute_percent_difference,
    olids_count_basis,
    extract_date,

    -- Acceptance criteria: <2% variance OR <5 persons difference
    case
        when emis_list_size is null or emis_list_size = 0 then false
        when absolute_percent_difference < 2 then true
        when absolute_difference < 5 then true
        else false
    end as meets_acceptance_criteria,

    -- Categorization for analysis
    case
        when emis_list_size is null or emis_list_size = 0 then 'Missing Data'
        when absolute_percent_difference < 2 or absolute_difference < 5 then 'Meets Criteria'
        when absolute_percent_difference < 5 then '2-5% Variance'
        when absolute_percent_difference < 20 then '5-20% Variance'
        else '20%+ Variance'
    end as variance_category,

    -- Validation methodology description
    'EMIS Regular Registration Comparison: <2% variance OR <5 persons difference (per-practice); <1% aggregate variance (all practices)' as validation_methodology

from comparison
order by
    case variance_category
        when '20%+ Variance' then 1
        when '5-20% Variance' then 2
        when '2-5% Variance' then 3
        when 'Meets Criteria' then 4
        else 5
    end,
    absolute_percent_difference desc nulls last,
    practice_code
