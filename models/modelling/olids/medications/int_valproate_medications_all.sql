{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'valproate', 'pregnancy_safety'])
}}

/*
All valproate medication orders for seizure management and bipolar disorder.
Uses two matching approaches:
1. Valproate programme codes lookup (legacy)
2. VALPROATE cluster_id from combined codesets
Critical for pregnancy safety monitoring and teratogenicity risk assessment.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH valproate_from_prog_codes AS (
    -- Get medication orders using valproate_prog_codes lookup
    SELECT DISTINCT
        mo.id AS medication_order_id,
        NULL AS medication_statement_id,
        mo.person_id,
        mo.clinical_effective_date::DATE AS order_date,
        mo.medication_name AS order_medication_name,
        mo.dose AS order_dose,
        mo.quantity_value AS order_quantity_value,
        mo.quantity_unit AS order_quantity_unit,
        mo.duration_days AS order_duration_days,
        NULL AS statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        mo.mapped_concept_id,
        vp.valproate_product_term,
        NULL AS bnf_code,
        NULL AS bnf_name,
        'PROG_CODES' AS source_method
    FROM {{ ref('stg_olids_medication_order') }} AS mo
    LEFT JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vp
        ON
            mo.mapped_concept_code = vp.code
            AND vp.code_category = 'DRUG'
    WHERE (
        -- Name-based matching for valproate
        mo.medication_name ILIKE '%VALPROATE%'
        OR mo.medication_name ILIKE '%VALPROIC ACID%'
    )
    OR (
        -- Concept ID matching via VALPROATE_PROG_CODES
        vp.code IS NOT NULL
    )
),

valproate_from_cluster AS (
    -- Get medication orders using VALPROATE cluster_id
    SELECT DISTINCT
        medication_order_id,
        medication_statement_id,
        person_id,
        order_date::DATE AS order_date,
        order_medication_name,
        order_dose,
        order_quantity_value,
        order_quantity_unit,
        order_duration_days,
        statement_medication_name,
        mapped_concept_code,
        mapped_concept_display,
        NULL AS mapped_concept_id,
        NULL AS valproate_product_term,
        bnf_code,
        bnf_name,
        'CLUSTER_ID' AS source_method
    FROM ({{ get_medication_orders(cluster_id='VALPROATE') }})
),

base_medication_orders AS (
    -- Combine both sources, deduplicating by medication_order_id
    SELECT
        COALESCE(pc.medication_order_id, cl.medication_order_id) AS medication_order_id,
        COALESCE(pc.medication_statement_id, cl.medication_statement_id) AS medication_statement_id,
        COALESCE(pc.person_id, cl.person_id) AS person_id,
        COALESCE(pc.order_date, cl.order_date) AS order_date,
        COALESCE(pc.order_medication_name, cl.order_medication_name) AS order_medication_name,
        COALESCE(pc.order_dose, cl.order_dose) AS order_dose,
        COALESCE(pc.order_quantity_value, cl.order_quantity_value) AS order_quantity_value,
        COALESCE(pc.order_quantity_unit, cl.order_quantity_unit) AS order_quantity_unit,
        COALESCE(pc.order_duration_days, cl.order_duration_days) AS order_duration_days,
        COALESCE(pc.statement_medication_name, cl.statement_medication_name) AS statement_medication_name,
        COALESCE(pc.mapped_concept_code, cl.mapped_concept_code) AS mapped_concept_code,
        COALESCE(pc.mapped_concept_display, cl.mapped_concept_display) AS mapped_concept_display,
        pc.mapped_concept_id,
        pc.valproate_product_term,
        COALESCE(pc.bnf_code, cl.bnf_code) AS bnf_code,
        COALESCE(pc.bnf_name, cl.bnf_name) AS bnf_name,
        CASE
            WHEN pc.medication_order_id IS NOT NULL AND cl.medication_order_id IS NOT NULL THEN 'BOTH'
            WHEN pc.medication_order_id IS NOT NULL THEN 'PROG_CODES'
            ELSE 'CLUSTER_ID'
        END AS source_method
    FROM valproate_from_prog_codes pc
    FULL OUTER JOIN valproate_from_cluster cl
        ON pc.medication_order_id = cl.medication_order_id
),

valproate_orders AS (
    SELECT
        bmo.*,

        -- Check for valproate name patterns (case-insensitive)
        (
            bmo.order_medication_name ILIKE '%VALPROATE%'
            OR bmo.order_medication_name ILIKE '%VALPROIC ACID%'
        ) AS matched_on_name,

        -- Concept ID matching flag
        (bmo.valproate_product_term IS NOT NULL) AS matched_on_concept_id,

        -- Extract specific valproate product information
        CASE
            WHEN
                bmo.valproate_product_term IS NOT NULL
                THEN bmo.valproate_product_term
            WHEN
                bmo.order_medication_name ILIKE '%SODIUM VALPROATE%'
                THEN 'SODIUM_VALPROATE'
            WHEN
                bmo.order_medication_name ILIKE '%VALPROIC ACID%'
                THEN 'VALPROIC_ACID'
            WHEN
                bmo.order_medication_name ILIKE '%EPILIM%' THEN 'EPILIM'
            WHEN
                bmo.order_medication_name ILIKE '%DEPAKOTE%' THEN 'DEPAKOTE'
            ELSE 'OTHER_VALPROATE'
        END AS valproate_product_type

    FROM base_medication_orders AS bmo
),

valproate_enhanced AS (
    SELECT
        vo.*,

        -- Clinical risk assessment flags
        TRUE AS is_high_teratogenic_risk,

        -- Pregnancy risk assessment
        CASE
            WHEN
                vo.valproate_product_type IN ('SODIUM_VALPROATE', 'EPILIM')
                THEN 'ANTI_EPILEPTIC'
            WHEN
                vo.valproate_product_type IN ('DEPAKOTE')
                THEN 'MOOD_STABILISER'
            ELSE 'UNSPECIFIED'
        END AS clinical_indication,

        -- Dosage categorisation for monitoring
        CASE
            WHEN vo.order_dose ILIKE '%MG%'
                THEN
                    CASE
                        WHEN
                            REGEXP_SUBSTR(vo.order_dose, '[0-9]+')::INT <= 500
                            THEN 'LOW_DOSE'
                        WHEN
                            REGEXP_SUBSTR(vo.order_dose, '[0-9]+')::INT <= 1000
                            THEN 'MODERATE_DOSE'
                        ELSE 'HIGH_DOSE'
                    END
            ELSE 'UNKNOWN_DOSE'
        END AS dose_category,

        -- Formulation type for bioequivalence monitoring
        CASE
            WHEN
                vo.order_medication_name ILIKE '%TABLET%' THEN 'TABLET'
            WHEN
                vo.order_medication_name ILIKE '%CAPSULE%' THEN 'CAPSULE'
            WHEN
                vo.order_medication_name ILIKE '%LIQUID%'
                OR vo.order_medication_name ILIKE '%SYRUP%'
                THEN 'LIQUID'
            WHEN
                vo.order_medication_name ILIKE '%MODIFIED RELEASE%'
                OR vo.order_medication_name ILIKE '%MR%'
                THEN 'MODIFIED_RELEASE'
            ELSE 'UNKNOWN_FORMULATION'
        END AS formulation_type,

        -- Recency flags for clinical monitoring
        vo.order_date >= CURRENT_DATE() - INTERVAL '3 months' AS is_recent_3m,
        vo.order_date >= CURRENT_DATE() - INTERVAL '6 months' AS is_recent_6m,
        vo.order_date >= CURRENT_DATE() - INTERVAL '12 months' AS is_recent_12m

    FROM valproate_orders AS vo
)

-- Final selection with ALL persons - no filtering by active status
-- Critical for pregnancy safety monitoring across all patient populations
SELECT
    ve.medication_order_id,
    ve.medication_statement_id,
    ve.person_id,
    ve.order_date,
    ve.order_medication_name,
    ve.order_dose,
    ve.order_quantity_value,
    ve.order_quantity_unit,
    ve.order_duration_days,
    ve.statement_medication_name,
    ve.mapped_concept_code,
    ve.mapped_concept_display,
    ve.mapped_concept_id,
    ve.valproate_product_term,
    ve.bnf_code,
    ve.bnf_name,
    ve.source_method,
    ve.matched_on_name,
    ve.matched_on_concept_id,
    ve.valproate_product_type,
    ve.is_high_teratogenic_risk,
    ve.clinical_indication,
    ve.dose_category,
    ve.formulation_type,
    ve.is_recent_3m,
    ve.is_recent_6m,
    ve.is_recent_12m,

    -- Add person demographics for reference
    p.current_practice_id,
    p.total_patients

FROM valproate_enhanced AS ve
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} AS p
    ON ve.person_id = p.person_id

-- Order by person and date for analysis
ORDER BY ve.person_id ASC, ve.order_date DESC
