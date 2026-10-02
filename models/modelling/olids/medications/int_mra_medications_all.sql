{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

-- Finerenone is excluded because it is not an HFrEF MRA in NICE NG106.
SELECT
    person_id,
    medication_order_id,
    order_date,
    bnf_code,
    bnf_name,
    mapped_concept_code,
    mapped_concept_display
FROM ({{ get_medication_orders(bnf_code='020203') }}) AS orders
WHERE bnf_code LIKE '0202030S0%' OR bnf_code LIKE '0202030X0%'
