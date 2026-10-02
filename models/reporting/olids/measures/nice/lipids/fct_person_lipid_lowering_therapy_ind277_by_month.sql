{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND277: https://www.nice.org.uk/indicators/ind277
-- Lipid-lowering therapy in the last 6 months for people on the diabetes register with type 1 diabetes, aged over 40, without haemorrhagic stroke history.
{{ nice_ind277('by_month') }}
