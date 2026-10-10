{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

-- NICE shared cvd risk profile at each completed month-end.
{{ calculate_nice_cvd_risk_profile('by_month') }}
