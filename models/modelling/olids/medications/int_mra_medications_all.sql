{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

SELECT
    person_id,
    medication_order_id,
    order_date,
    bnf_code,
    bnf_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code LIKE '0202030S0%' OR bnf_code LIKE '0202030X0%' AS is_steroidal_mra
FROM ({{ get_medication_orders(bnf_code='020203') }}) AS orders
WHERE bnf_code LIKE '0202030S0%' OR bnf_code LIKE '0202030X0%' OR bnf_code LIKE '0202030Y0%'
