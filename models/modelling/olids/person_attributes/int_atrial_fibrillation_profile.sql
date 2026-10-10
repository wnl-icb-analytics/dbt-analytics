{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE shared atrial fibrillation profile at the current reference date.
{{ calculate_nice_atrial_fibrillation_profile('current') }}
