{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND158: https://www.nice.org.uk/indicators/ind158
-- Total cholesterol to HDL ratio recorded in 12 months for adults with an active SMI diagnosis, excluding cardiovascular disease diagnosed more than 12 months ago.
{{ nice_ind158('by_month') }}
