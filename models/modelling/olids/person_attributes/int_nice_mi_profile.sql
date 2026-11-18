{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_mi_profile('current') }}
