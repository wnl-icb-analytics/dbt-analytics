{{ config(materialized='table') }}

-- NICE IND225: https://www.nice.org.uk/indicators/ind225
-- Two MenB doses before 8 months of age for babies who reached 8 months in the preceding 12 months.
{{ nice_ind225('current') }}
