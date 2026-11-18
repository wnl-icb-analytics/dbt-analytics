{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND169: https://www.nice.org.uk/indicators/ind169
-- Anticoagulant medication review in 12 months for people over 18 on the AF register with an oral anticoagulant order in the preceding 6 months.
{{ nice_ind169('by_month') }}
