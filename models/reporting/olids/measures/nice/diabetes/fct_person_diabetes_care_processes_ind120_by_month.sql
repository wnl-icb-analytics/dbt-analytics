{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND120: https://www.nice.org.uk/indicators/ind120
-- NICE corrected the renal process to eGFR creatinine measurement in February 2026.
-- Count HbA1c tests with or without a value, and performed foot and ACR tests
-- anywhere in the period, even if a later foot record is declined or the ACR
-- test has no numeric result.
{{ nice_ind120('by_month') }}
