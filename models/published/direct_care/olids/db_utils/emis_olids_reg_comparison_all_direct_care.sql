{{
    config(
        materialized='table',
        alias='emis_olids_reg_comparison_all',
        tags=['data_quality', 'published', 'direct_care']
    )
}}

/*
EMIS/OLIDS Practice Registration Comparison - All Practices (Direct Care)

Complete comparison of all practices using EMIS acceptance criteria.
Compares OLIDS Regular registrations against EMIS list size.

Categories:
- Meets Criteria: <2% variance OR <5 persons difference
- 2-5% Variance: Between 2-5% variance
- 5-20% Variance: Between 5-20% variance
- 20%+ Variance: 20% or greater variance
- Missing Data: No EMIS or OLIDS data available

Acceptance Criteria:
- Aggregate: <1% variance across all practices
- Per-practice: <2% variance OR fewer than 5 persons difference

Use Cases:
- Comprehensive data quality monitoring for Regular registrations
- Practice-level completeness assessment
- Acceptance criteria validation
- Monthly reporting

PowerBI Usage:
- Connect to PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED schema
- Table alias: emis_olids_practice_comparison_all
- Filter by variance_category for different views
- Group by variance_category to see distribution
- Calculate aggregate variance to check <1% threshold
*/

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
    extract_date as snapshot_date,
    meets_acceptance_criteria,
    variance_category
from {{ ref('int_emis_olids_practice_registration_comparison') }}
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
