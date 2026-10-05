{{ config(materialized='view') }}

-- NICE IND161: https://www.nice.org.uk/indicators/ind161
-- CVD risk assessment within 3 months either side of a first hypertension or type 2 diabetes diagnosis in the preceding 12 months, aged 25 to 84; excludes CVD, CKD, FH and type 1 diabetes.
{{ nice_ind161('current') }}
