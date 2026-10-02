{{ config(materialized='view') }}

-- NICE IND215: https://www.nice.org.uk/indicators/ind215
-- Three DTP-containing doses before 8 months of age for babies who reached 8 months in the preceding 12 months.
{{ nice_ind215('current') }}
