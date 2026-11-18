{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND143: https://www.nice.org.uk/indicators/ind143
-- Mental health care plan recorded in 12 months and on or after the relapse (or first diagnosis) for people with an active SMI diagnosis.
{{ nice_ind143('by_month') }}
