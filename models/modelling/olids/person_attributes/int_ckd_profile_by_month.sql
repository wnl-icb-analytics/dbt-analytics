{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

-- NICE shared ckd profile at each completed month-end.
{{ calculate_nice_ckd_profile('by_month') }}
