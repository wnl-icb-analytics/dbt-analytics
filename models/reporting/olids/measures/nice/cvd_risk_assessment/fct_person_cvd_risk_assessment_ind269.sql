{{ config(materialized='table') }}

-- NICE IND269: https://www.nice.org.uk/indicators/ind269
-- CVD risk assessment recorded in 5 years for people aged 45 to 84; excludes type 1 diabetes, CVD, FH, CKD, current lipid-lowering therapy and a risk score of 20% or more ever.
{{ nice_ind269('current') }}
