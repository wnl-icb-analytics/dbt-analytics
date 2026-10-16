{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All inhaled corticosteroid medication orders for respiratory conditions.
Uses BNF classification (3.2) for inhaled corticosteroids.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    base_orders.person_id,
    base_orders.medication_order_id,
    base_orders.medication_statement_id,
    base_orders.order_date,
    base_orders.order_medication_name,
    base_orders.order_dose,
    base_orders.order_quantity_value,
    base_orders.order_quantity_unit,
    base_orders.order_duration_days,
    base_orders.statement_medication_name,
    base_orders.mapped_concept_code,
    base_orders.mapped_concept_display,
    base_orders.bnf_code,
    base_orders.bnf_name,

    -- A combination has more than one current dm+d ingredient, including triple inhalers.
    CASE WHEN dmd.ingredient_count = 1 THEN 'SINGLE_AGENT'
         WHEN dmd.ingredient_count >= 2 THEN 'COMBINATION'
         ELSE 'OTHER_ICS' END AS ics_type,
    dmd.ingredient_count = 1 AS is_single_agent,
    dmd.ingredient_count >= 2 AS is_combination_therapy,

    -- Order recency flags (ICS are typically long-term therapy)
    {{ medication_date_flags('base_orders.order_date') }}

FROM ({{ get_medication_orders(bnf_code='0302') }}) base_orders
LEFT JOIN {{ ref('dmd_product') }} dmd ON base_orders.mapped_concept_code = dmd.product_code
ORDER BY base_orders.person_id, base_orders.order_date DESC
