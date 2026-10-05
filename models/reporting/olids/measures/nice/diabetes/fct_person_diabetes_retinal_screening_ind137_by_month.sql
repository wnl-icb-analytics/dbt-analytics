{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND137: https://www.nice.org.uk/indicators/ind137
-- Retinal screening recorded in 12 months on the diabetes register.
{{ nice_ind137('by_month') }}
