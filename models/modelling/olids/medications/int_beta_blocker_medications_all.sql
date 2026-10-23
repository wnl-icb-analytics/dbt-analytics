{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All beta blocker medication orders for cardiovascular protection and rate control.
Uses the national licensed and unlicensed beta-blocker drug refsets.
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

    -- Order recency flags (beta blockers are typically long-term therapy)
    {{ medication_date_flags('order_date') }},
    cluster_id = 'NHS_DRUG_REFSETS/LBB_COD' AS is_hf_licensed

FROM (
    {{ get_medication_orders(cluster_id=['NHS_DRUG_REFSETS/LBB_COD', 'NHS_DRUG_REFSETS/ULBB_COD'], source='OPENCODELISTS') }}
) base_orders
ORDER BY person_id, order_date DESC
