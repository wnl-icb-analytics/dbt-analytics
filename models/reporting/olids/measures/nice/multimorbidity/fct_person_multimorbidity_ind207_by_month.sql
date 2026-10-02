{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND207: https://www.nice.org.uk/indicators/ind207
-- Structured medication review in 12 months for people with moderate or severe coded frailty or conditions in four or more
-- NICE IND207 categories.
{{ nice_ind207('by_month') }}
