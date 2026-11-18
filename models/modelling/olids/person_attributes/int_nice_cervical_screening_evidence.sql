{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_cervical_screening_evidence('current') }}
