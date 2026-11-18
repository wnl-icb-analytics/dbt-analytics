{{ config(materialized='table') }}

{{ nice_asthma_union('current') }}
