{{ config(materialized='table') }}

-- NICE IND135: https://www.nice.org.uk/indicators/ind135
-- Last HbA1c in 12 months at or below 64 mmol/mol, whole diabetes register.
{{ nice_ind135('current') }}
