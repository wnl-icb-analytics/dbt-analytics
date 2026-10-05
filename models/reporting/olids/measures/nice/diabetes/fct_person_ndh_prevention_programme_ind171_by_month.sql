{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND171: https://www.nice.org.uk/indicators/ind171
-- Referral (made or declined) to the NHS Diabetes Prevention Programme for adults newly diagnosed with non-diabetic hyperglycaemia in the preceding 12 months, excluding unresolved diabetes.
{{ nice_ind171('by_month') }}
