{{ config(materialized='table') }}

{{ nice_weight_management_union('current') }}
