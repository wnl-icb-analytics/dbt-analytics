{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_alcohol_evidence('current') }}
