{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND240: hypertension blood pressure control for people aged 80 years and over.
{{ nice_ind240('by_month') }}
