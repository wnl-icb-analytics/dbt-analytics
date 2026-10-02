{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND213: https://www.nice.org.uk/indicators/ind213
-- Cervical screening completed in 3.5 years for women aged 25 to 49 with an active SMI diagnosis.
{{ nice_ind213('by_month') }}
