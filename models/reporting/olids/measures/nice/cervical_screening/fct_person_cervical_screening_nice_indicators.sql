{{ config(materialized='table') }}

{{ nice_cervical_screening_union('current') }}
