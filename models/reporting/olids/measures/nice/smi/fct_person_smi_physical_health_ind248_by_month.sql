{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND248: https://www.nice.org.uk/indicators/ind248
-- All six physical health checks in 12 months for people with an active SMI diagnosis.
{{ nice_ind248('by_month') }}
