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

    -- Legacy classification retained pending approval; corticosteroid_type_dmd is ingredient-based.
    CASE
        WHEN base_orders.bnf_code LIKE '060301%' THEN 'REPLACEMENT'        -- Replacement therapy
        WHEN base_orders.bnf_code LIKE '060302%' THEN 'GLUCOCORTICOID'     -- Glucocorticoid therapy
        WHEN base_orders.bnf_code LIKE '060303%' THEN 'MINERALOCORTICOID'  -- Mineralocorticoid
        ELSE 'OTHER_CORTICOSTEROID'
    END AS corticosteroid_type,


    -- Usage classification flags
    CASE WHEN base_orders.bnf_code LIKE '060301%' THEN TRUE ELSE FALSE END AS is_replacement_therapy,
    CASE WHEN base_orders.bnf_code LIKE '060302%' THEN TRUE ELSE FALSE END AS is_anti_inflammatory,
    CASE WHEN dmd.vtm_id IS NULL THEN NULL
         ELSE dmd.vtm_id IN ('775983003', '89192008') END AS is_mineralocorticoid,



    -- Order recency flags (important for steroid monitoring)
    {{ medication_date_flags('base_orders.order_date', months=[1, 3, 6]) }},
    -- Current and previous fludrocortisone VTM identifiers in dm+d.
    CASE WHEN dmd.vtm_id IN ('775983003', '89192008') THEN 'MINERALOCORTICOID'
         WHEN dmd.vtm_id IS NOT NULL THEN 'GLUCOCORTICOID'
         ELSE 'UNKNOWN' END AS corticosteroid_type_dmd

FROM ({{ get_medication_orders(bnf_code='0603') }}) base_orders
LEFT JOIN {{ ref('dmd_product') }} dmd ON base_orders.mapped_concept_code = dmd.product_code
ORDER BY base_orders.person_id, base_orders.order_date DESC
