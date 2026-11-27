{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

SELECT
    orders.person_id,
    orders.medication_order_id,
    orders.order_date,
    orders.bnf_code,
    orders.bnf_name,
    orders.mapped_concept_code,
    orders.mapped_concept_display,
    -- Current and previous finerenone VTM identifiers in dm+d.
    CASE WHEN dmd.vtm_id IS NULL THEN NULL
         ELSE dmd.vtm_id NOT IN ('1163351002', '40634211000001108') END AS is_steroidal_mra
FROM ({{ get_medication_orders(cluster_id='NHS_DRUG_REFSETS/MRADRUG_COD', source='OPENCODELISTS') }}) AS orders
LEFT JOIN {{ ref('dmd_product') }} dmd ON orders.mapped_concept_code = dmd.product_code
