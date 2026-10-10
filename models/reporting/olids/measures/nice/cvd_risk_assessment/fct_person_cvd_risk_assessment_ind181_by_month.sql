{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND181: https://www.nice.org.uk/indicators/ind181
-- CVD risk assessment recorded in 3 years for people aged 25 to 84 with type 2 diabetes, no moderate or severe frailty and no statin order in 6 months; excludes CVD, FH and CKD.
{{ nice_ind181('by_month') }}
