{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND88: https://www.nice.org.uk/indicators/ind88
-- Referral to a structured education programme within 9 months of joining the diabetes register, for people diagnosed in the preceding 12 months.
{{ nice_ind88('by_month') }}
