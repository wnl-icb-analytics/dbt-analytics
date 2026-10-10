{{ config(materialized='table') }}

-- NICE IND229: https://www.nice.org.uk/indicators/ind229
-- Lipid-lowering therapy in 6 months for people aged 25 to 84 whose last recorded CVD risk score is 10% or more, without established CVD.
{{ nice_ind229('current') }}
