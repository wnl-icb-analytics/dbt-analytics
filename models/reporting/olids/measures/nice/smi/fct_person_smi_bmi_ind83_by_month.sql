{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND83: https://www.nice.org.uk/indicators/ind83
-- BMI recorded in 15 months for people with an active SMI diagnosis.
{{ nice_ind83('by_month') }}
