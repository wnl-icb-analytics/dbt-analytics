{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND245: peripheral arterial disease BP control for people aged 79 years and under.
{{ nice_ind245('by_month') }}
