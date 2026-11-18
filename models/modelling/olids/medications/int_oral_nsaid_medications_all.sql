{{ config(materialized='table', cluster_by=['person_id', 'order_date']) }}

SELECT
    medication_order_id,
    person_id,
    order_date
FROM ({{ get_medication_orders(cluster_id='NHS_DRUG_REFSETS/ORALNSAIDDRUG_COD', source='OPENCODELISTS') }}) AS orders
