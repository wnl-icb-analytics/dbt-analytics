{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'diabetes', 'dpp4'])
}}

/*
All DPP-4 inhibitor (gliptin) medication orders using the DPP4I_RX umbrella cluster,
including fixed-dose combinations (with metformin, or with an SGLT2 inhibitor).
Each order is categorised down to its active ingredient via the per-drug sub-clusters.
Membership is cluster-driven and date-independent; date filtering is applied by
consumers (fct/pit models) and recency flags are provided for convenience.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH dpp4_orders_base AS (
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
    FROM ({{ get_medication_orders(cluster_id='DPP4I_RX') }}) mo
),

-- Per-ingredient sub-clusters used to label each order by active ingredient.
-- Mutually exclusive at the code level (one gliptin per product, incl. combinations),
-- so the join below does not fan out orders.
drug_clusters AS (
    SELECT DISTINCT
        code AS mapped_concept_code,
        cluster_id AS sub_cluster_id
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE UPPER(cluster_id) IN (
        'SITAGLIPTIN_RX', 'LINAGLIPTIN_RX', 'SAXAGLIPTIN_RX', 'VILDAGLIPTIN_RX', 'ALOGLIPTIN_RX'
    )
),

dpp4_enhanced AS (
    SELECT
        dob.person_id,
        dob.medication_order_id,
        dob.medication_statement_id,
        dob.order_date,
        dob.order_medication_name,
        dob.order_dose,
        dob.order_quantity_value,
        dob.order_quantity_unit,
        dob.order_duration_days,
        dob.statement_medication_name,
        dob.mapped_concept_code,
        dob.mapped_concept_display,
        dob.bnf_chapter,
        dob.bnf_code,
        dob.bnf_name,
        'DPP4I_RX' AS cluster_id,
        dc.sub_cluster_id,

        -- DPP-4 inhibitor active ingredient
        CASE dc.sub_cluster_id
            WHEN 'SITAGLIPTIN_RX' THEN 'SITAGLIPTIN'
            WHEN 'LINAGLIPTIN_RX' THEN 'LINAGLIPTIN'
            WHEN 'SAXAGLIPTIN_RX' THEN 'SAXAGLIPTIN'
            WHEN 'VILDAGLIPTIN_RX' THEN 'VILDAGLIPTIN'
            WHEN 'ALOGLIPTIN_RX' THEN 'ALOGLIPTIN'
            ELSE 'OTHER_DPP4I'
        END AS dpp4_drug,

        -- Order recency flags (DPP-4 inhibitors are typically long-term therapy)
        {{ medication_date_flags('dob.order_date') }}
    FROM dpp4_orders_base dob
    LEFT JOIN drug_clusters dc
        ON dob.mapped_concept_code = dc.mapped_concept_code
),

dpp4_with_counts AS (
    SELECT
        de.*,
        COUNT(*) OVER (
            PARTITION BY de.person_id
            ORDER BY de.order_date
            RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
        ) AS rolling_order_count_12m
    FROM dpp4_enhanced de
)

SELECT
    dwc.*,
    p.current_practice_id,
    p.total_patients
FROM dpp4_with_counts dwc
LEFT JOIN {{ ref('dim_person') }} p
    ON dwc.person_id = p.person_id
ORDER BY dwc.person_id, dwc.order_date DESC
