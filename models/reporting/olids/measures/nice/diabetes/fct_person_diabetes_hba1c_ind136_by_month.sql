{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND136: https://www.nice.org.uk/indicators/ind136
-- Last HbA1c in 12 months at or below 75 mmol/mol, whole diabetes register.
{{ nice_ind136('by_month') }}
