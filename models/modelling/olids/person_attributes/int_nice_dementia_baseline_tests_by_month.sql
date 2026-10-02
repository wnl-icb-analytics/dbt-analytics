{{ config(materialized='table', cluster_by=['person_id', 'reporting_date'], tags=['monthly-full', 'nice-history']) }}

{{ calculate_nice_dementia_baseline_tests('by_month') }}
