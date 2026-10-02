{{ config(materialized='view') }}

-- NICE IND111: https://www.nice.org.uk/indicators/ind111
-- Urine ACR recorded in 15 months on the diabetes register; a recorded test counts with or without a value.
{{ nice_ind111('current') }}
