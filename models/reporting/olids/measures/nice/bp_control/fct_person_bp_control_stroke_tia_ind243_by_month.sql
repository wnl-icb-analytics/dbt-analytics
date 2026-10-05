{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND243: stroke/TIA blood pressure control for people aged 79 years and under.
{{ nice_ind243('by_month') }}
