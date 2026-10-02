{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

{{ calculate_nice_asthma_risk_medications() }}
