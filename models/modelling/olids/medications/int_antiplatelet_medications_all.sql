{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All antiplatelet medication orders for cardiovascular protection and thrombosis prevention.
Uses BNF classification (2.9) for antiplatelet drugs.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
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


    -- P2Y12 inhibitor classification (for dual antiplatelet therapy)
    CASE
        WHEN bnf_code LIKE '0209000C0%' THEN TRUE
        WHEN bnf_code LIKE '0209000Y0%' THEN TRUE
        WHEN bnf_code LIKE '0209000Z0%' THEN TRUE
        ELSE FALSE
    END AS is_p2y12_inhibitor,

    -- Order recency flags (antiplatelets are typically long-term therapy)
    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m,

    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS is_recent_12m,
    bnf_code LIKE '0209000A0%' AS is_aspirin,
    bnf_code LIKE '0209000C0%' AS is_clopidogrel

FROM (
    {{ get_medication_orders(bnf_code='0209') }}
) base_orders
ORDER BY person_id, order_date DESC
