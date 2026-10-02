{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND82: https://www.nice.org.uk/indicators/ind82
-- Alcohol consumption recorded in 15 months for people with an active SMI diagnosis.
{{ nice_ind82('by_month') }}
