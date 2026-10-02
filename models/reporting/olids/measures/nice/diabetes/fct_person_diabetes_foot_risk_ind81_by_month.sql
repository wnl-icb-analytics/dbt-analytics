{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND81: https://www.nice.org.uk/indicators/ind81
-- Foot examination with a risk classification in 15 months on the diabetes register, allowing an absent or amputated foot in the examination but excluding anyone with a foot amputation.
{{ nice_ind81('by_month') }}
