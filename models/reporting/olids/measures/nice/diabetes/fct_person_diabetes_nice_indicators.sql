{{ config(materialized='table') }}

-- Common long-form interface for the NICE diabetes, NDH and gestational diabetes indicator views.
{{ nice_diabetes_union('current') }}
