{{ config(materialized='view') }}

-- NICE IND141: https://www.nice.org.uk/indicators/ind141
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the COPD register; excludes a persisting contraindication or one recorded in 12 months.
{{ nice_ind141('current') }}
