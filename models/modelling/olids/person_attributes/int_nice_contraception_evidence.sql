{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_contraception_evidence('current') }}
