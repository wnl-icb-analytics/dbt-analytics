{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

{{ calculate_nice_weight_management_referral_evidence('by_month') }}
