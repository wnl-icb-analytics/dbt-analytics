{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE control evidence at the last 60 completed month-ends.
{{ calculate_nice_blood_pressure_latest('by_month') }}
