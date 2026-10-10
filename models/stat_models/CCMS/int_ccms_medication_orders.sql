{{
    config(
        materialized='view')
}}

-- CCMS medication evidence, one row per order and matching condition.
-- Preserve the current lower window without an upper cap; lithium is all-time.

SELECT
    person_id,
    medication_order_id,
    order_date,
    mapped_concept_code,
    conditionid,
    conditionname
FROM {{ ref('int_ccms_medication_orders_all') }}
WHERE order_date >= DATEADD('month', -12, CURRENT_DATE())
    OR conditionid = 5029
