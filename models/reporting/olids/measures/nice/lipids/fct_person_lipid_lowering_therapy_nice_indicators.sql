{{ config(materialized='table') }}

-- Common long-form interface for the NICE lipid-lowering therapy indicator views.
{{ nice_lipids_union('current') }}
