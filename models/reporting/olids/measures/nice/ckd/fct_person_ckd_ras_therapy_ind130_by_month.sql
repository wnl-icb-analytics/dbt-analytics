{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND130: https://www.nice.org.uk/indicators/ind130
-- ACE inhibitor or ARB order in 6 months for people on the CKD and hypertension registers with proteinuria; excludes people contraindicated to both classes.
{{ nice_ind130('by_month') }}
