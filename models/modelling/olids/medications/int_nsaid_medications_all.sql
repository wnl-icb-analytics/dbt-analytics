{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All NSAID (Non-Steroidal Anti-Inflammatory Drug) medication orders for pain and inflammation.
Uses BNF 10.1.1; the previous BNF 10.1 scope could not include topical products.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH medication_attributes AS (
    SELECT orders.*, dmd.vtm_id,
    -- Current and previous coxib VTMs; meloxicam is not COX-2 selective.
    CASE WHEN dmd.vtm_id IS NULL THEN NULL
         ELSE dmd.vtm_id IN (
             '775123001', '370187009', '775901004', '395292007',
             '777080002', '395289008', '776587002', '422506000',
             '777459002', '116095002', '777908009', '385583007') END AS is_cox2_selective
    FROM ({{ get_medication_orders(bnf_code='100101') }}) orders
    LEFT JOIN {{ ref('dmd_product') }} dmd ON orders.mapped_concept_code = dmd.product_code
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

    CASE WHEN base_orders.vtm_id IS NULL THEN 'OTHER_NSAID'
         WHEN base_orders.is_cox2_selective THEN 'COX2_SELECTIVE'
         ELSE 'NON_SELECTIVE' END AS nsaid_type,
    base_orders.is_cox2_selective,
    NOT base_orders.is_cox2_selective AS is_non_selective,

    -- Order recency flags
    {{ medication_date_flags('base_orders.order_date') }}

FROM medication_attributes base_orders
ORDER BY base_orders.person_id, base_orders.order_date DESC
