{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND156: https://www.nice.org.uk/indicators/ind156
-- Smoking status recorded in 12 months for people with a listed LTC; a never-smoker reaching 26 by the end of the financial year is covered by a never-smoked record made after their 25th birthday and after their earliest listed diagnosis.
{{ nice_ind156('by_month') }}
