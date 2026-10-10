{{ config(materialized='table') }}

{{ nice_copd_union('current') }}
