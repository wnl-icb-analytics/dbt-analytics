{{ config(materialized='view') }}

-- NICE IND172: https://www.nice.org.uk/indicators/ind172
-- HbA1c or fasting plasma glucose in 12 months on the NDH register.
{{ nice_ind172('current') }}
