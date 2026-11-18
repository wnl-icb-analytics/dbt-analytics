{{ config(materialized='table') }}

{{ nice_flu_vaccination_union('current') }}
