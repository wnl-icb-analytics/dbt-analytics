{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND134: https://www.nice.org.uk/indicators/ind134
-- ACE inhibitor or ARB order in 6 months for diabetes with proteinuria or microalbuminuria; excludes contraindications to both classes.
{{ nice_ind134('by_month') }}
