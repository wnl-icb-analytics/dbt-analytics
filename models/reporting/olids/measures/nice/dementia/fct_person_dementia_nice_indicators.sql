{{ config(materialized='table') }}

{{ nice_dementia_union('current') }}
