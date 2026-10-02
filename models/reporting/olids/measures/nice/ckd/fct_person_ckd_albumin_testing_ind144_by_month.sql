{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND144: https://www.nice.org.uk/indicators/ind144
-- Urine ACR or PCR recorded in 12 months on the CKD register (stage 3 to 5 by code).
{{ nice_ind144('by_month') }}
