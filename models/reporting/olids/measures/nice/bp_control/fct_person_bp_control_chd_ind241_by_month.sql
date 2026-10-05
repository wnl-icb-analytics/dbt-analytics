{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND241: coronary heart disease BP control for people aged 79 years and under.
{{ nice_ind241('by_month') }}
