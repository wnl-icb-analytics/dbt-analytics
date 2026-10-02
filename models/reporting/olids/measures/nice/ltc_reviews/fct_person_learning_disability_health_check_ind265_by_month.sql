{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND265: https://www.nice.org.uk/indicators/ind265
-- Learning disability health check and health action plan both recorded in 12 months for people on the learning disability register.
{{ nice_ind265('by_month') }}
