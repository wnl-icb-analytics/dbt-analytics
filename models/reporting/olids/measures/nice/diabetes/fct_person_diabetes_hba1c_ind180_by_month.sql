{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND180: https://www.nice.org.uk/indicators/ind180
-- Last HbA1c in 12 months at or below 75 mmol/mol, diabetes register with moderate or severe frailty.
{{ nice_ind180('by_month') }}
