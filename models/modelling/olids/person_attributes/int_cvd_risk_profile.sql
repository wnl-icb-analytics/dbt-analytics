{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE shared cvd risk profile at the current reference date.
{{ calculate_nice_cvd_risk_profile('current') }}
