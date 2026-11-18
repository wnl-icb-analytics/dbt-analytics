{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_physical_health_evidence('current') }}
