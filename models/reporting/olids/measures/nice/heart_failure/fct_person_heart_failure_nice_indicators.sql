{{ config(materialized='table') }}

{{ nice_heart_failure_union('current') }}
