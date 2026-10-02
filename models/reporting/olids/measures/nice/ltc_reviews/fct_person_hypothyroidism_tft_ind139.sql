{{ config(materialized='view') }}

-- NICE IND139: https://www.nice.org.uk/indicators/ind139
-- Thyroid function test recorded in 12 months for people on the hypothyroidism register.
{{ nice_ind139('current') }}
