{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND198: https://www.nice.org.uk/indicators/ind198
-- FAST or AUDIT-C screen within 3 months either side of a first depression or anxiety diagnosis in the preceding 12 months, aged 10 and over; excludes alcohol-related disorders.
{{ nice_ind198('by_month') }}
