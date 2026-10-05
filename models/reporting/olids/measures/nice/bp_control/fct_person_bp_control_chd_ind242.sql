{{ config(materialized='table') }}

-- NICE IND242: coronary heart disease BP control for people aged 80 years and over.
{{ nice_ind242('current') }}
