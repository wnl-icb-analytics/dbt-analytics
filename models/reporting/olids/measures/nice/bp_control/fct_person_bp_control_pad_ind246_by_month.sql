{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND246: peripheral arterial disease BP control for people aged 80 years and over.
{{ nice_ind246('by_month') }}
