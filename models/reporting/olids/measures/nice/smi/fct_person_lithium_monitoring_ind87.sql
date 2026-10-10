{{ config(materialized='table') }}

-- NICE IND87: https://www.nice.org.uk/indicators/ind87
-- Latest serum lithium recorded in 4 months and in the 0.4 to 1.0 mmol/L range for people on lithium therapy.
{{ nice_ind87('current') }}
