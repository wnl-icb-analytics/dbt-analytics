{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All lithium medication orders.
Uses the LIT_COD cluster (QOF lithium prescription codes) per MH2_REG.
BNF 040203 is intentionally NOT used: that paragraph ("drugs used for mania
and hypomania") also covers non-lithium agents (valproate, carbamazepine,
antimanic antipsychotics), which do not count as lithium for the SMI register.
*/

WITH cluster_orders AS (
    SELECT
        person_id, medication_order_id, medication_statement_id,
        order_date, date_recorded, order_medication_name, order_dose,
        order_quantity_value, order_quantity_unit, order_duration_days,
        statement_medication_name, mapped_concept_code, mapped_concept_display,
        bnf_code, bnf_name
    FROM ({{ get_medication_orders(cluster_id='LIT_COD') }}) co
    QUALIFY ROW_NUMBER() OVER (PARTITION BY medication_order_id ORDER BY mapped_concept_code) = 1
)

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
    date_recorded,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code,
    bnf_name,
    {{ medication_date_flags('order_date') }}
FROM cluster_orders
ORDER BY person_id, order_date DESC
