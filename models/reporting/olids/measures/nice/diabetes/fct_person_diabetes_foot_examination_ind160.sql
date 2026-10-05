{{ config(materialized='view') }}

-- NICE IND160: https://www.nice.org.uk/indicators/ind160
-- Monofilament foot sensation testing in 12 months on the diabetes register.
{{ nice_ind160('current') }}
