{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND163: https://www.nice.org.uk/indicators/ind163
-- Flu vaccination in the most recently completed season (1 August to 31 March) for people on the diabetes register; excludes a persisting contraindication or one recorded in 12 months.
{{ nice_ind163('by_month') }}
