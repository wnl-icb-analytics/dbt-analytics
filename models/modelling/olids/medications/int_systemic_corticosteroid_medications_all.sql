{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
Systemic corticosteroid medication orders in BNF 0603; indication is not established by product.
Uses BNF classification (6.3) for corticosteroids and corticotropins.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH classified_orders AS (
    SELECT base_orders.*,
        -- Current and previous fludrocortisone VTM identifiers in dm+d; null means unknown.
        dmd.vtm_id IN ('775983003', '89192008') AS is_mineralocorticoid
    FROM ({{ get_medication_orders(bnf_code='0603') }}) base_orders
    LEFT JOIN {{ ref('dmd_product') }} dmd ON base_orders.mapped_concept_code = dmd.product_code
)

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

    -- Keeps the published column width from the retired OTHER_* label.
    CASE WHEN is_mineralocorticoid THEN 'MINERALOCORTICOID'
         WHEN NOT is_mineralocorticoid THEN 'GLUCOCORTICOID'
         ELSE 'UNKNOWN' END::varchar(20) AS corticosteroid_type,

    -- Compatibility name used by C-LTCS for glucocorticoid course evidence, not indication.
    NOT is_mineralocorticoid AS is_anti_inflammatory,
    is_mineralocorticoid,

    -- Order recency flags (important for steroid monitoring)
    {{ medication_date_flags('base_orders.order_date', months=[1, 3, 6]) }},
    -- Keeps the existing 17-character width; corticosteroid_type is wider because of the retired OTHER_CORTICOSTEROID label.
    corticosteroid_type::varchar(17) AS corticosteroid_type_dmd

FROM classified_orders base_orders
ORDER BY base_orders.person_id, base_orders.order_date DESC
