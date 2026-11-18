{{ config(materialized='table') }}

-- NICE IND152: https://www.nice.org.uk/indicators/ind152
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the CHD, stroke/TIA, diabetes or COPD register.
{{ nice_ind152('current') }}
