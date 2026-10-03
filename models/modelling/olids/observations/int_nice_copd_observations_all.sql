{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

{{ calculate_nice_copd_observations() }}
