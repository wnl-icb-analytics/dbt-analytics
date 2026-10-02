{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND239: hypertension blood pressure control for people aged 79 years and under.
{{ nice_ind239('by_month') }}
