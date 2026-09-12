{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/* Lipid-lowering medication orders used by QOF OBES2. */

WITH matched_orders AS (
    SELECT
        person_id,
        medication_order_id,
        medication_statement_id,
        order_date,
        date_recorded,
        order_medication_name,
        mapped_concept_code,
        mapped_concept_display,
        cluster_id AS source_cluster_id
    FROM (
        {{ get_medication_orders(cluster_id=[
            'STAT_COD',
            'BEMPACID_COD',
            'EZETIMIBE_COD',
            'INCLISIRAN_COD',
            'PCSK9I_COD'
        ], source='ECL_CACHE') }}
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY medication_order_id, cluster_id
        ORDER BY mapped_concept_code
    ) = 1
)

SELECT *
FROM matched_orders
