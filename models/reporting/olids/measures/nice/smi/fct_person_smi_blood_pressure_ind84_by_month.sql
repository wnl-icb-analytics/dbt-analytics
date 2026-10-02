{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND84: https://www.nice.org.uk/indicators/ind84
-- Blood pressure recorded in 15 months for people with an active SMI diagnosis.
{{ nice_ind84('by_month') }}
