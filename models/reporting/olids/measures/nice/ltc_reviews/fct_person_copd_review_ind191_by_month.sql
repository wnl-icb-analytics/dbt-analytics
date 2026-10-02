{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND191: https://www.nice.org.uk/indicators/ind191
-- COPD review, exacerbation count and MRC dyspnoea assessment in 12 months for people on the COPD register.
{{ nice_ind191('by_month') }}
