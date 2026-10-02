{{ config(materialized='view') }}

-- NICE IND245: peripheral arterial disease BP control for people aged 79 years and under.
{{ nice_ind245('current') }}
