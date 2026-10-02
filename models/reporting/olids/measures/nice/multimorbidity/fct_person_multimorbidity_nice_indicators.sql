{{ config(materialized='table') }}

-- Common long-form interface for the NICE multimorbidity indicator views.
{{ nice_multimorbidity_union('current') }}
