{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND274: https://www.nice.org.uk/indicators/ind274
-- Lipid-lowering therapy in 6 months for people aged 25 to 84 with type 2 diabetes, a CVD risk score of 10% or more recorded in the preceding 12 months, no CVD and no moderate or severe frailty.
{{ nice_ind274('by_month') }}
