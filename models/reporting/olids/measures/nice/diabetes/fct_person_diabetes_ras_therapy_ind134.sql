{{ config(materialized='view') }}

-- NICE IND134: https://www.nice.org.uk/indicators/ind134
-- ACE inhibitor or ARB order in 6 months for diabetes with proteinuria or microalbuminuria; excludes contraindications to both classes.
{{ nice_ind134('current') }}
