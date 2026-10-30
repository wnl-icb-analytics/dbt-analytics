{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Valproate 6-Month Latest Intermediate Model
-- Single Responsibility: Latest valproate medication order per person in last 6 months
-- Critical for clinical safety monitoring, especially pregnancy + valproate combinations

WITH valproate_6m_orders AS (
    SELECT
        orders.person_id,
        orders.medication_order_id,
        orders.medication_statement_id,
        orders.order_date,
        orders.order_medication_name,
        orders.order_dose,
        orders.order_quantity_value,
        orders.order_quantity_unit,
        orders.order_duration_days,
        orders.statement_medication_name,
        orders.mapped_concept_code,
        orders.mapped_concept_display,
        orders.date_recorded,

        -- Valproate product classification
        CASE
            WHEN order_medication_name ILIKE '%EPILIM%' THEN 'EPILIM'
            WHEN order_medication_name ILIKE '%CONVULEX%' THEN 'CONVULEX'
            WHEN order_medication_name ILIKE '%DEPAKOTE%' THEN 'DEPAKOTE'
            WHEN order_medication_name ILIKE '%EPISENTA%' THEN 'EPISENTA'
            WHEN
                order_medication_name ILIKE '%VALPROATE%'
                THEN 'VALPROATE_GENERIC'
            ELSE 'OTHER_VALPROATE'
        END AS valproate_product_term

    FROM {{ ref('int_valproate_medications_all') }} AS orders
    -- The inclusive lower boundary is a deliberate exception that retains the published cohort.
    WHERE
        order_date >= DATEADD(month, -6, CURRENT_DATE())
        AND order_date <= CURRENT_DATE()
),

valproate_6m_counts AS (
    SELECT
        person_id,
        COUNT(*) AS recent_order_count
    FROM valproate_6m_orders
    GROUP BY person_id
),

valproate_latest AS (
    SELECT
        vo.*,
        vc.recent_order_count,
        ROW_NUMBER()
            -- Orders have equal clinical priority. Prefer the latest EMIS save on the same day.
            OVER (PARTITION BY vo.person_id ORDER BY
                vo.order_date DESC,
                vo.date_recorded DESC NULLS LAST,
                vo.medication_order_id DESC)
            AS rn
    FROM valproate_6m_orders AS vo
    INNER JOIN valproate_6m_counts AS vc ON vo.person_id = vc.person_id
)

SELECT
    person_id,
    order_date AS most_recent_order_date,
    medication_order_id,
    medication_statement_id,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    valproate_product_term,
    recent_order_count
FROM valproate_latest
WHERE rn = 1 -- Latest order only per person
