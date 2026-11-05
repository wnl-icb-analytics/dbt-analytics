{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'diabetes', 'metformin'])
}}

/*
All metformin medication orders using the METFORMIN_RX cluster, including fixed-dose
combinations (with a DPP-4 inhibitor, SGLT2 inhibitor or thiazolidinedione).
Metformin is a single ingredient so there is no per-drug categorisation; the value of
this model over BNF 6.1.2.2 is that it captures combination products (which BNF codes
under 6.1.2.3 and so miss).
Membership is cluster-driven and date-independent; date filtering is applied by
consumers (fct/pit models) and recency flags are provided for convenience.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH metformin_orders_base AS (
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
        mo.bnf_name,
        'METFORMIN_RX' AS cluster_id,
        -- A combination product contains metformin plus another antidiabetic ingredient
        -- (caught here but coded by BNF under 6.1.2.3 rather than the biguanide paragraph).
        -- NULL bnf_code is left unknown rather than assumed to be a combination.
        CASE
            WHEN mo.bnf_code IS NULL THEN NULL
            WHEN mo.bnf_code LIKE '0601022%' THEN FALSE
            ELSE TRUE
        END AS is_combination
    FROM ({{ get_medication_orders(cluster_id='METFORMIN_RX') }}) mo
),

metformin_enhanced AS (
    SELECT
        mob.*,
        {{ medication_date_flags('mob.order_date') }}
    FROM metformin_orders_base mob
),

metformin_with_counts AS (
    SELECT
        me.*,
        COUNT(*) OVER (
            PARTITION BY me.person_id
            ORDER BY me.order_date
            RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
        ) AS rolling_order_count_12m
    FROM metformin_enhanced me
)

SELECT
    mwc.*,
    p.current_practice_id,
    p.total_patients
FROM metformin_with_counts mwc
LEFT JOIN {{ ref('dim_person') }} p
    ON mwc.person_id = p.person_id
ORDER BY mwc.person_id, mwc.order_date DESC
