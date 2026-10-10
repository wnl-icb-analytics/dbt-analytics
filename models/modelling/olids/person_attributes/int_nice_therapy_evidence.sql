{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_therapy_evidence('current') }}
