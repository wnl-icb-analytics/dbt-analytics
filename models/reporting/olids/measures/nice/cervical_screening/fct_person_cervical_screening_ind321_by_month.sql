{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND321: https://www.nice.org.uk/indicators/ind321
-- Cervical screening recorded in 5.5 years for women aged 25 to 64; excludes people without a cervix.
{{ nice_ind321('by_month') }}
