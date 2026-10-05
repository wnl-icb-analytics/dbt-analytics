{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND244: stroke/TIA blood pressure control for people aged 80 years and over.
{{ nice_ind244('by_month') }}
