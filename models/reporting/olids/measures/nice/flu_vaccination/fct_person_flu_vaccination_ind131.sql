{{ config(materialized='view') }}

-- NICE IND131: https://www.nice.org.uk/indicators/ind131
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the CHD register; excludes a persisting contraindication or one recorded in 12 months.
{{ nice_ind131('current') }}
