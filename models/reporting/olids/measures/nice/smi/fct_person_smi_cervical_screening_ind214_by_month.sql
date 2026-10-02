{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND214: https://www.nice.org.uk/indicators/ind214
-- Cervical screening completed in 5.5 years for women aged 50 to 64 with an active SMI diagnosis.
{{ nice_ind214('by_month') }}
