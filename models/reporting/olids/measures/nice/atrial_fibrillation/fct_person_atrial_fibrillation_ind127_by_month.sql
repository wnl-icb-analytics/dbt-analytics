{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND127: https://www.nice.org.uk/indicators/ind127
-- CHA2DS2-VASc assessment in 12 months, excluding a latest eligible stroke risk score of 2 or more before the period.
{{ nice_ind127('by_month') }}
