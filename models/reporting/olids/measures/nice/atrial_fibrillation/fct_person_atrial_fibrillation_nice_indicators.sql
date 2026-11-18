{{ config(materialized='table') }}

{{ nice_atrial_fibrillation_union('current') }}
