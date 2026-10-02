{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND273: https://www.nice.org.uk/indicators/ind273
-- Asthma review in 12 months with a same-day written plan and an exacerbation count in the preceding month, for people aged 5 and over.
{{ nice_ind273('by_month') }}
