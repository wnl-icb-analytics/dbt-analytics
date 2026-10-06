{{ config(materialized='table') }}

-- NICE IND164: https://www.nice.org.uk/indicators/ind164
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the stroke/TIA register, before personalised care adjustments.
{{ nice_ind164('current') }}
