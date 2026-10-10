{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND229: https://www.nice.org.uk/indicators/ind229
-- Lipid-lowering therapy in 6 months for people aged 25 to 84 whose last recorded CVD risk score is 10% or more, without established CVD.
{{ nice_ind229('by_month') }}
