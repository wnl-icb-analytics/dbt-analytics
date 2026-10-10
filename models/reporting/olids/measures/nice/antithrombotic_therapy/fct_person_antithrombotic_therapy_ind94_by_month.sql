{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND94: https://www.nice.org.uk/indicators/ind94
-- Antiplatelet order or recorded OTC salicylate use in 15 months on the PAD register, excluding people with an oral anticoagulant order in the same period.
{{ nice_ind94('by_month') }}
