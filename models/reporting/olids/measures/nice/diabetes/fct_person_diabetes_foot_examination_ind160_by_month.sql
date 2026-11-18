{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND160: https://www.nice.org.uk/indicators/ind160
-- Monofilament foot sensation testing in 12 months on the diabetes register.
{{ nice_ind160('by_month') }}
