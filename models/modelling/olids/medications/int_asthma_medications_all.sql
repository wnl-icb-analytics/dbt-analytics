{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'asthma', 'qof'])
}}

/*
All asthma medication orders using ASTTRT_COD cluster.
Date filtering should be applied by consumers (fct/pit models).
Includes window counts and recency flags for convenience.
*/

WITH asthma_orders_base AS (
    SELECT
        mo.person_id,
        mo.medication_order_id,
        mo.order_date,
        mo.date_recorded,
        mo.order_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        'ASTTRT_COD' AS cluster_id
    FROM ({{ get_medication_orders(cluster_id='ASTTRT_COD') }}) mo
    WHERE mo.order_date <= CURRENT_DATE()
),

asthma_enhanced AS (
    SELECT
        aob.*,
        TRUE AS is_asthma_treatment,
        aob.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        aob.order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
        aob.order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m
    FROM asthma_orders_base aob
),

asthma_with_counts AS (
    SELECT
        ae.*,
        COUNT(*) OVER (
            PARTITION BY ae.person_id
            ORDER BY ae.order_date
            RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
        ) AS rolling_order_count_12m
    FROM asthma_enhanced ae
)

SELECT
    awc.*,
    p.current_practice_id,
    p.total_patients
FROM asthma_with_counts awc
LEFT JOIN {{ ref('dim_person') }} p
    ON awc.person_id = p.person_id
ORDER BY awc.person_id, awc.order_date DESC
