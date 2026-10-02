{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND263: https://www.nice.org.uk/indicators/ind263
-- ACE inhibitor or ARB order in 6 months for people on the CKD register with a latest ACR of 70 mg/mmol or more and no diabetes.
{{ nice_ind263('by_month') }}
