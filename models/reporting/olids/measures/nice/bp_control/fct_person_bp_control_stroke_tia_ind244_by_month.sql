{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND244: stroke/TIA blood pressure control for people aged 80 years and over.
{{ nice_ind244('by_month') }}
