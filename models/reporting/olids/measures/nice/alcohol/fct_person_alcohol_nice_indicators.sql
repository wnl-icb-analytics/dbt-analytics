{{ config(materialized='table') }}

-- Common long-form interface for the NICE alcohol indicator views.
{{ nice_alcohol_union('current') }}
