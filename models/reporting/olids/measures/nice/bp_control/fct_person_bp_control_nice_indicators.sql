{{ config(materialized='table') }}

-- Common long-form interface for the NICE BP control indicator views.
{{ nice_bp_control_union('current') }}
