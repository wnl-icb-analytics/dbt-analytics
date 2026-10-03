{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

SELECT person_id, medication_order_id, order_date::DATE AS order_date
FROM ({{ get_medication_orders(cluster_id='ICS_FORMOTEROL_MART_RX', source='ECL_CACHE') }})
WHERE order_date::DATE <= CURRENT_DATE()
