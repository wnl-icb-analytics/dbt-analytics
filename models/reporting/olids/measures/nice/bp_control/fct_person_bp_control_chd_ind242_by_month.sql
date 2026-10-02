{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND242: coronary heart disease BP control for people aged 80 years and over.
{{ nice_ind242('by_month') }}
