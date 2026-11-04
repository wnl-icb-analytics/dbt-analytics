{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full']) }}

-- National Diabetes Audit treatment targets at each of the last 60 completed month-ends.
{{ calculate_diabetes_triple_target('by_month') }}
