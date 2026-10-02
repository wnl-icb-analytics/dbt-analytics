{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

-- NICE shared atrial fibrillation profile at each completed month-end.
{{ calculate_nice_atrial_fibrillation_profile('by_month') }}
