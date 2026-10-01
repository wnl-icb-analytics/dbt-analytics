{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medications', 'antimicrobial']
    )
}}

/*
Person-level antimicrobial medication summary.

Antimicrobial use counts over rolling 3, 6 and 12 month windows, plus distinct
active ingredients (VTM) prescribed in the last 12 months.

Source: int_antibacterial_medications_all (BNF chapter 5.1 antibacterials) — primary care
Grain: One row per person with at least one antibacterial order in the last 12 months

Planned extensions (placeholders):
- Secondary care antimicrobial prescribing (SUS / hospital e-prescribing)
- Primary care infection presentations (GP diagnoses / encounters)
- Secondary care infection presentations (SUS A&E, outpatient, inpatient)
*/

WITH antibacterial_orders AS (
    SELECT
        ab.person_id,
        ab.medication_order_id,
        ab.order_date,
        ab.is_recent_3m,
        ab.is_recent_6m,
        ab.is_recent_12m,
        bnf.vtm AS vtm_code,
        bnf.vtm_name AS active_ingredient_name
    FROM {{ ref('int_antibacterial_medications_all') }} AS ab
    LEFT JOIN {{ ref('stg_reference_bnf_latest') }} AS bnf
        ON ab.mapped_concept_code = bnf.snomed_code
    WHERE ab.is_recent_12m = TRUE
),

order_counts AS (
    SELECT
        person_id,
        COUNT(DISTINCT CASE WHEN is_recent_3m THEN medication_order_id END) AS antimicrobial_order_count_3mo,
        COUNT(DISTINCT CASE WHEN is_recent_6m THEN medication_order_id END) AS antimicrobial_order_count_6mo,
        COUNT(DISTINCT medication_order_id) AS antimicrobial_order_count_12mo
    FROM antibacterial_orders
    GROUP BY person_id
),

active_ingredients_12mo AS (
    SELECT
        person_id,
        COUNT(DISTINCT vtm_code) AS unique_active_ingredient_count_12mo,
        ARRAY_COMPACT(
            ARRAY_AGG(DISTINCT active_ingredient_name)
            WITHIN GROUP (ORDER BY active_ingredient_name)
        ) AS active_ingredients_12mo
    FROM antibacterial_orders
    WHERE vtm_code IS NOT NULL
    GROUP BY person_id
)

SELECT
    oc.person_id,

    -- Primary care antimicrobial prescribing
    oc.antimicrobial_order_count_3mo,
    oc.antimicrobial_order_count_6mo,
    oc.antimicrobial_order_count_12mo,
    COALESCE(ai.unique_active_ingredient_count_12mo, 0) AS unique_active_ingredient_count_12mo,
    COALESCE(ai.active_ingredients_12mo, ARRAY_CONSTRUCT()) AS active_ingredients_12mo,

    -- TO DO: secondary care antimicrobial prescribing
    NULL::NUMBER AS antimicrobial_secondary_care_order_count_3mo,
    NULL::NUMBER AS antimicrobial_secondary_care_order_count_6mo,
    NULL::NUMBER AS antimicrobial_secondary_care_order_count_12mo,
    NULL::NUMBER AS unique_active_ingredient_secondary_care_count_12mo,
    NULL::ARRAY AS active_ingredients_secondary_care_12mo,

    -- TO DO: primary care infection presentations
    NULL::NUMBER AS infection_presentation_count_primary_care_3mo,
    NULL::NUMBER AS infection_presentation_count_primary_care_6mo,
    NULL::NUMBER AS infection_presentation_count_primary_care_12mo,

    -- TO DO: secondary care infection presentations
    NULL::NUMBER AS infection_presentation_count_secondary_care_3mo,
    NULL::NUMBER AS infection_presentation_count_secondary_care_6mo,
    NULL::NUMBER AS infection_presentation_count_secondary_care_12mo

FROM order_counts AS oc
LEFT JOIN active_ingredients_12mo AS ai
    ON oc.person_id = ai.person_id
