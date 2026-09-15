{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'diabetes', 'glp1'])
}}

/*
All GLP-1 receptor agonist medication orders using the GLP1RA_RX umbrella cluster
(all GLP-1 RAs, including the dual GIP/GLP-1 agonist tirzepatide).
Each order is categorised down to its active ingredient via the per-drug sub-clusters.
Captures orders regardless of indication (diabetes BNF 6.1 vs obesity BNF 4.5);
bnf_code/bnf_chapter are retained so consumers can split by indication.
Date filtering should be applied by consumers (fct/pit models).
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH glp1_orders_base AS (
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
    FROM ({{ get_medication_orders(cluster_id='GLP1RA_RX') }}) mo
),

-- Per-ingredient sub-clusters used to label each order by active ingredient.
-- These are mutually exclusive at the code level (one active ingredient per product),
-- so the join below does not fan out orders.
drug_clusters AS (
    SELECT DISTINCT
        code AS mapped_concept_code,
        cluster_id AS sub_cluster_id
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE UPPER(cluster_id) IN (
        'SEMAGLUTIDE_RX', 'DULAGLUTIDE_RX', 'LIRAGLUTIDE_RX', 'EXENATIDE_RX',
        'LIXISENATIDE_RX', 'ALBIGLUTIDE_RX', 'TIRZEPATIDE_RX'
    )
),

glp1_enhanced AS (
    SELECT
        gob.person_id,
        gob.medication_order_id,
        gob.medication_statement_id,
        gob.order_date,
        gob.order_medication_name,
        gob.order_dose,
        gob.order_quantity_value,
        gob.order_quantity_unit,
        gob.order_duration_days,
        gob.statement_medication_name,
        gob.mapped_concept_code,
        gob.mapped_concept_display,
        gob.bnf_chapter,
        gob.bnf_code,
        gob.bnf_name,
        'GLP1RA_RX' AS cluster_id,
        dc.sub_cluster_id,

        -- GLP-1 RA active ingredient
        CASE dc.sub_cluster_id
            WHEN 'SEMAGLUTIDE_RX' THEN 'SEMAGLUTIDE'
            WHEN 'DULAGLUTIDE_RX' THEN 'DULAGLUTIDE'
            WHEN 'LIRAGLUTIDE_RX' THEN 'LIRAGLUTIDE'
            WHEN 'EXENATIDE_RX' THEN 'EXENATIDE'
            WHEN 'LIXISENATIDE_RX' THEN 'LIXISENATIDE'
            WHEN 'ALBIGLUTIDE_RX' THEN 'ALBIGLUTIDE'
            WHEN 'TIRZEPATIDE_RX' THEN 'TIRZEPATIDE'
            ELSE 'OTHER_GLP1RA'
        END AS glp1_drug,

        -- Tirzepatide is a dual GIP/GLP-1 receptor agonist (distinct mechanism)
        CASE WHEN dc.sub_cluster_id = 'TIRZEPATIDE_RX' THEN TRUE ELSE FALSE END AS is_dual_gip_glp1,

        -- BNF chapter indicates likely indication: 06 = diabetes, 04 = obesity/CNS
        CASE WHEN gob.bnf_code LIKE '0601%' THEN TRUE ELSE FALSE END AS is_diabetes_indication,

        -- Order recency flags (GLP-1 RAs are typically long-term therapy); future-dated orders are not recent
        gob.order_date BETWEEN CURRENT_DATE() - INTERVAL '3 months' AND CURRENT_DATE() AS is_recent_3m,
        gob.order_date BETWEEN CURRENT_DATE() - INTERVAL '6 months' AND CURRENT_DATE() AS is_recent_6m,
        gob.order_date BETWEEN CURRENT_DATE() - INTERVAL '12 months' AND CURRENT_DATE() AS is_recent_12m
    FROM glp1_orders_base gob
    LEFT JOIN drug_clusters dc
        ON gob.mapped_concept_code = dc.mapped_concept_code
),

glp1_with_counts AS (
    SELECT
        ge.*,
        COUNT(*) OVER (
            PARTITION BY ge.person_id
            ORDER BY ge.order_date
            RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
        ) AS rolling_order_count_12m
    FROM glp1_enhanced ge
)

SELECT
    gwc.*,
    p.current_practice_id,
    p.total_patients
FROM glp1_with_counts gwc
LEFT JOIN {{ ref('dim_person') }} p
    ON gwc.person_id = p.person_id
ORDER BY gwc.person_id, gwc.order_date DESC
