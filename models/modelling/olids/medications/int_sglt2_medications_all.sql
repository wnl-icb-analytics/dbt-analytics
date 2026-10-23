{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'diabetes', 'sglt2'])
}}

/*
All SGLT2 inhibitor medication orders using the SGLT2I_RX umbrella cluster
(all SGLT2 inhibitors, including fixed-dose combinations with metformin or a DPP-4).
Each order is categorised down to its active ingredient via the per-drug sub-clusters.
SGLT2 inhibitors all sit in BNF 6.1 but are prescribed across diabetes, heart failure
and CKD; clinical indication is resolved from the patient's registers, not the drug,
so no indication flag is set here.
Date filtering should be applied by consumers (fct/pit models).
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH sglt2_orders_base AS (
    SELECT
        mo.person_id,
        mo.medication_order_id,
        mo.medication_statement_id,
        mo.order_date,
        mo.order_medication_name,
        mo.order_dose,
        mo.order_quantity_value,
        mo.order_quantity_unit,
        mo.order_duration_days,
        mo.statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        mo.bnf_chapter,
        mo.bnf_code,
        mo.bnf_name
    FROM ({{ get_medication_orders(cluster_id='SGLT2I_RX') }}) mo
),

-- Per-ingredient sub-clusters used to label each order by active ingredient.
-- Mutually exclusive at the code level (one gliflozin per product, incl. combinations),
-- so the join below does not fan out orders.
drug_clusters AS (
    SELECT DISTINCT
        code AS mapped_concept_code,
        cluster_id AS sub_cluster_id
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE UPPER(cluster_id) IN (
        'DAPAGLIFLOZIN_RX', 'EMPAGLIFLOZIN_RX', 'CANAGLIFLOZIN_RX', 'ERTUGLIFLOZIN_RX'
    )
),

sglt2_enhanced AS (
    SELECT
        sob.person_id,
        sob.medication_order_id,
        sob.medication_statement_id,
        sob.order_date,
        sob.order_medication_name,
        sob.order_dose,
        sob.order_quantity_value,
        sob.order_quantity_unit,
        sob.order_duration_days,
        sob.statement_medication_name,
        sob.mapped_concept_code,
        sob.mapped_concept_display,
        sob.bnf_chapter,
        sob.bnf_code,
        sob.bnf_name,
        'SGLT2I_RX' AS cluster_id,
        dc.sub_cluster_id,

        -- SGLT2 inhibitor active ingredient
        CASE dc.sub_cluster_id
            WHEN 'DAPAGLIFLOZIN_RX' THEN 'DAPAGLIFLOZIN'
            WHEN 'EMPAGLIFLOZIN_RX' THEN 'EMPAGLIFLOZIN'
            WHEN 'CANAGLIFLOZIN_RX' THEN 'CANAGLIFLOZIN'
            WHEN 'ERTUGLIFLOZIN_RX' THEN 'ERTUGLIFLOZIN'
            ELSE 'OTHER_SGLT2I'
        END AS sglt2_drug,

        -- Order recency flags (SGLT2 inhibitors are typically long-term therapy)
        {{ medication_date_flags('sob.order_date') }}
    FROM sglt2_orders_base sob
    LEFT JOIN drug_clusters dc
        ON sob.mapped_concept_code = dc.mapped_concept_code
),

sglt2_with_counts AS (
    SELECT
        se.*,
        COUNT(*) OVER (
            PARTITION BY se.person_id
            ORDER BY se.order_date
            RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
        ) AS rolling_order_count_12m
    FROM sglt2_enhanced se
)

SELECT
    swc.*,
    p.current_practice_id,
    p.total_patients
FROM sglt2_with_counts swc
LEFT JOIN {{ ref('dim_person') }} p
    ON swc.person_id = p.person_id
ORDER BY swc.person_id, swc.order_date DESC
