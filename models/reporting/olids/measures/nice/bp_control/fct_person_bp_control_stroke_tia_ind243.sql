{{ config(materialized='table') }}

-- NICE IND243: stroke/TIA blood pressure control for people aged 79 years and under.
{{ nice_ind243('current') }}
