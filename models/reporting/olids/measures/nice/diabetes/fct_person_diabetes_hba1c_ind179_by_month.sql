{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND179: https://www.nice.org.uk/indicators/ind179
-- Last HbA1c in 12 months at or below 58 mmol/mol, diabetes register without moderate or severe frailty.
{{ nice_ind179('by_month') }}
