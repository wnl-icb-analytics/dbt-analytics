{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND233: https://www.nice.org.uk/indicators/ind233
-- eGFR on two occasions at least 90 days apart, the second within 90 days before diagnosis, for people diagnosed with CKD stage 3 to 5 in the preceding 12 months.
{{ nice_ind233('by_month') }}
