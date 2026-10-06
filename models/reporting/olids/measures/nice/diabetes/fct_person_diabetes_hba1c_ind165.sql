{{ config(materialized='table') }}

-- NICE IND165: https://www.nice.org.uk/indicators/ind165
-- Last HbA1c in 12 months at or below 58 mmol/mol, whole diabetes register.
{{ nice_ind165('current') }}
