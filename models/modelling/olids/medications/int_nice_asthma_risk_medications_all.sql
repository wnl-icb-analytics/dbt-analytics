{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

WITH saba_orders AS (
    SELECT DISTINCT medication_order_id
    FROM ({{ get_medication_orders(cluster_id='OPENSAFELY/SABA_INHALER_MEDICATIONS', source='OPENCODELISTS') }})
)
SELECT m.person_id, m.medication_order_id, m.order_date::DATE AS order_date,
    m.bnf_code LIKE '0603020T0%' AS is_prednisolone,
    -- Quantities are doses: Accuhaler has 60, Turbohaler 100, other inhalers 200.
    -- Count at least one device per matched SABA order, including missing quantities.
    IFF(s.medication_order_id IS NOT NULL,
        GREATEST(1, ROUND(COALESCE(m.quantity_value, 0) /
            CASE WHEN m.medication_name ILIKE '%accuhaler%' THEN 60
                 WHEN m.medication_name ILIKE '%turbohaler%' THEN 100
                 ELSE 200 END)), 0)::NUMBER AS saba_inhaler_count
FROM {{ ref('int_medication_order_bnf') }} m
LEFT JOIN saba_orders s ON m.medication_order_id = s.medication_order_id
WHERE s.medication_order_id IS NOT NULL OR m.bnf_code LIKE '0603020T0%'
