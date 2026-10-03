{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}
WITH contraceptive_orders AS (
    {{ get_medication_orders(cluster_id=['CONTRACEP_COC_RX', 'CONTRACEP_POP_RX', 'CONTRACEP_PATCH_RX', 'CONTRACEP_EHC_RX'], source='ECL_CACHE') }}
)
SELECT orders.person_id, orders.medication_order_id, orders.order_date::DATE AS order_date,
    selected.cluster_id AS source_cluster_id,
    orders.mapped_concept_code, orders.mapped_concept_display,
    orders.issue_method,
    COALESCE(orders.issue_method IN ('Electronic', 'Print', 'Handwritten'), FALSE) AS is_practice_issued
FROM {{ ref('int_medication_order_bnf') }} AS orders
INNER JOIN contraceptive_orders AS selected ON orders.medication_order_id = selected.medication_order_id
