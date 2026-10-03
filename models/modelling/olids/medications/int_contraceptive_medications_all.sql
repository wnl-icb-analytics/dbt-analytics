{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}
WITH codes AS (
    SELECT DISTINCT code, cluster_id
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'ECL_CACHE'
        AND cluster_id IN ('CONTRACEP_COC_RX', 'CONTRACEP_POP_RX', 'CONTRACEP_PATCH_RX', 'CONTRACEP_EHC_RX')
)
SELECT orders.person_id, orders.medication_order_id, orders.order_date::DATE AS order_date,
    codes.cluster_id AS source_cluster_id,
    orders.mapped_concept_code, orders.mapped_concept_display,
    orders.issue_method,
    COALESCE(orders.issue_method IN ('Electronic', 'Print', 'Handwritten'), FALSE) AS is_practice_issued
FROM {{ ref('int_medication_order_bnf') }} AS orders
INNER JOIN codes ON orders.mapped_concept_code = codes.code
