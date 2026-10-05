{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'epilepsy', 'seizure_management'])
}}

/*
All epilepsy medication orders for anti-epileptic drugs.
Uses cluster ID EPILDRUG_COD.
Date filtering should be applied by consumers (fct/pit models).
*/

SELECT
    person_id,
    patient_id,
    sk_patient_id,
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
    cluster_id,
    order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
    order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
    order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m
FROM (
    {{ get_medication_orders(cluster_id='EPILDRUG_COD') }}
) base_orders
ORDER BY person_id, order_date DESC
