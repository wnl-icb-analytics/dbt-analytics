{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_hba1c_evidence('current') }}
