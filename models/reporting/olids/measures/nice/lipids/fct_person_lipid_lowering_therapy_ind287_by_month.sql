{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND287: https://www.nice.org.uk/indicators/ind287
-- Lipid-lowering therapy in 6 months for people aged 25 to 84 with hypertension or type 2 diabetes first diagnosed in the preceding 12 months and a CVD risk score of 10% or more in that period; excludes CVD, CKD, FH and type 1 diabetes.
{{ nice_ind287('by_month') }}
