{{ config(materialized='table') }}

-- Common long-form interface for the NICE smoking indicator views.
{{ nice_smoking_union('current') }}
