{{ config(materialized='table', cluster_by=['person_id', 'reporting_date']) }}

{{ calculate_nice_dementia_baseline_tests('current') }}
