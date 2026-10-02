{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_review_evidence('current') }}
