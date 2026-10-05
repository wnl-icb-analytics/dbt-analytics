{{ config(materialized='table') }}

-- NICE IND239: hypertension blood pressure control for people aged 79 years and under.
{{ nice_ind239('current') }}
