{{ config(materialized='table') }}

{{ nice_hypertension_union('current') }}
