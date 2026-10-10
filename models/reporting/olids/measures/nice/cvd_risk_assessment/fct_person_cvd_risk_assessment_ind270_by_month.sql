{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND270: https://www.nice.org.uk/indicators/ind270
-- CVD risk assessment recorded in 3 years for people aged 43 to 84 who smoke, have obesity, hypertension or a latest total cholesterol above 5 mmol/L; same exclusions as IND269.
{{ nice_ind270('by_month') }}
