{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_weight_management_referral_evidence('current') }}
