{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

SELECT person_id, medication_order_id, order_date, date_recorded,
    mapped_concept_code, mapped_concept_display, bnf_code, bnf_name
FROM ({{ get_medication_orders(cluster_id='LEVOTHYROXINE_RX', source='ECL_CACHE') }})
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY medication_order_id ORDER BY mapped_concept_code DESC
) = 1
