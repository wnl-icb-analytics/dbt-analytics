{{ config(materialized='view') }}

-- NICE IND157: https://www.nice.org.uk/indicators/ind157
-- Offer of smoking cessation support recorded in 12 months for current smokers with a listed LTC.
{{ nice_ind157('current') }}
