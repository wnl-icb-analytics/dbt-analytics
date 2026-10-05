{{ config(materialized='view') }}

-- NICE IND171: https://www.nice.org.uk/indicators/ind171
-- Referral (made or declined) to the NHS Diabetes Prevention Programme for adults newly diagnosed with non-diabetic hyperglycaemia in the preceding 12 months, excluding unresolved diabetes.
{{ nice_ind171('current') }}
