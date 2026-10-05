{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND208: https://www.nice.org.uk/indicators/ind208
-- Asked about falls in 12 months for people aged 65 and over with moderate or severe coded frailty.
{{ nice_ind208('by_month') }}
