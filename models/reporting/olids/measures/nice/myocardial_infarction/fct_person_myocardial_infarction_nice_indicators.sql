{{ config(materialized='table') }}

{{ nice_myocardial_infarction_union('current') }}
