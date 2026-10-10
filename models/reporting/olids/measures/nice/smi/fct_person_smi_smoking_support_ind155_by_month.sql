{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND155: https://www.nice.org.uk/indicators/ind155
-- Offer of smoking cessation support in 12 months for current smokers with an active SMI diagnosis.
{{ nice_ind155('by_month') }}
