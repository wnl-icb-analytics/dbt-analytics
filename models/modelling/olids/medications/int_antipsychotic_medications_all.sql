{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All antipsychotic medication orders for mental health conditions.
Uses BNF classification (4.2.1 and 4.2.2) for drugs used in psychoses and related disorders.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Do not include Lithium as this is already catpured in int_lithium_medications_all
*/

--date that medication data was first acquired from the Practice systems
WITH Medsupload as (
SELECT 
    MAX(lds_transform_datetime) AS lds_transform_datetime
    FROM {{ ref('stg_olids_medication_order') }}
)
--Chapter 4.2.1 Antipsychotic Drugs
,drugs as (
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
    'Antipyschotic Drugs' as antipyschotic_class
FROM (
    {{ get_medication_orders(bnf_code='040201') }}
) base_orders
--ORDER BY person_id, order_date DESC
)
--Chapter 4.2.2 Depot Injections
,depot as (
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
    'Antipyschotic Depot Injections' as antipyschotic_class
FROM (
    {{ get_medication_orders(bnf_code='040202') }}
) base_orders
--ORDER BY person_id, order_date DESC
) 
SELECT DISTINCT a.*
-- Antipyschotic class flags
    ,CASE WHEN bnf_code LIKE '040201%' THEN TRUE ELSE FALSE END AS is_antipsychotic_drug
    ,CASE WHEN bnf_code LIKE '040202%' THEN TRUE ELSE FALSE END AS is_depot_injection
  -- Order recency flags 
    ,CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m
    ,CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m
    ,CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS is_recent_12m
FROM (
    SELECT * FROM drugs
    UNION ALL
    SELECT * FROM depot
) a

--ensure there are orders only up to the data acquired date
WHERE ORDER_DATE <= (SELECT lds_transform_datetime from Medsupload)
ORDER BY person_id, order_date DESC
