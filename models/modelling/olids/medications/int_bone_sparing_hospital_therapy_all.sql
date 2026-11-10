{{ config(materialized='table', cluster_by=['person_id', 'therapy_date']) }}

WITH medication_orders AS (
    SELECT * FROM {{ ref('int_bone_sparing_medications_all') }}
), medication_statements AS (
    {{ get_medication_statements(cluster_id='NHS_DRUG_REFSETS/BSADRUG_COD', source='OPENCODELISTS') }}
), bone_products AS (
    SELECT DISTINCT codes.product_code, codes.bnf_code, product.unit_dose_strength_mg,
        CASE
            WHEN product.vtm_id IN ('777988006', '134600006') OR codes.bnf_code LIKE '0606020V0%'
                THEN 'ZOLEDRONIC_ACID'
            WHEN product.vtm_id = '775477008' OR codes.bnf_code LIKE '0606020Z0%'
                THEN 'DENOSUMAB'
        END AS therapy_type
    FROM (
        SELECT mapped_concept_code AS product_code, bnf_code FROM medication_orders
        UNION
        SELECT mapped_concept_code, bnf_code FROM medication_statements
    ) AS codes
    LEFT JOIN {{ ref('dmd_product') }} AS product ON codes.product_code = product.product_code
    WHERE therapy_type IS NOT NULL
), observations AS (
    SELECT person_id, id::VARCHAR AS record_id, clinical_effective_date::DATE AS therapy_date,
        date_recorded, 'PCD_OBSERVATION' AS therapy_source, mapped_concept_code, mapped_concept_display,
        CASE
            WHEN mapped_concept_display ILIKE '%zoledronic%' THEN 'ZOLEDRONIC_ACID'
            WHEN mapped_concept_display ILIKE '%intravenous%bisphosphonate%' THEN 'INTRAVENOUS_BISPHOSPHONATE'
            WHEN mapped_concept_display ILIKE '%denosumab%' THEN 'DENOSUMAB'
            ELSE 'OTHER_BONE_SPARING_THERAPY'
        END AS therapy_type,
        'THERAPY_CODE' AS evidence_type, FALSE AS is_authorisation_only,
        TRUE AS has_administration_evidence, NULL::BOOLEAN AS is_outside_prescription,
        NULL::FLOAT AS unit_dose_strength_mg
    FROM ({{ get_observations("'BSA_COD'", source='PCD') }})
    WHERE clinical_effective_date IS NOT NULL
), orders AS (
    SELECT orders.person_id, orders.medication_order_id::VARCHAR AS record_id,
        orders.order_date::DATE AS therapy_date, orders.date_recorded,
        'MEDICATION_ORDER' AS therapy_source, orders.mapped_concept_code, orders.mapped_concept_display,
        product.therapy_type,
        'PRESCRIPTION' AS evidence_type, FALSE AS is_authorisation_only,
        FALSE AS has_administration_evidence,
        source_order.issue_method_description ILIKE 'Outside%' AS is_outside_prescription,
        product.unit_dose_strength_mg
    FROM medication_orders AS orders
    LEFT JOIN {{ ref('stg_olids_medication_order') }} AS source_order
        ON orders.medication_order_id = source_order.id
    INNER JOIN bone_products AS product ON EQUAL_NULL(orders.mapped_concept_code, product.product_code)
        AND EQUAL_NULL(orders.bnf_code, product.bnf_code)
), authorisations AS (
    SELECT statement.person_id, statement.medication_statement_id::VARCHAR AS record_id,
        statement.statement_date::DATE AS therapy_date, statement.date_recorded,
        'MEDICATION_STATEMENT' AS therapy_source, statement.mapped_concept_code, statement.mapped_concept_display,
        product.therapy_type,
        'AUTHORISATION_ONLY' AS evidence_type, TRUE AS is_authorisation_only,
        FALSE AS has_administration_evidence, NULL::BOOLEAN AS is_outside_prescription,
        product.unit_dose_strength_mg
    FROM medication_statements AS statement
    INNER JOIN bone_products AS product ON EQUAL_NULL(statement.mapped_concept_code, product.product_code)
        AND EQUAL_NULL(statement.bnf_code, product.bnf_code)
    WHERE statement.cancellation_date IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('stg_olids_medication_order') }} AS linked_order
            WHERE linked_order.medication_statement_id = statement.medication_statement_id
                AND linked_order.clinical_effective_date IS NOT NULL
        )
), therapy AS (
    SELECT * FROM observations
    UNION ALL
    SELECT * FROM orders
    UNION ALL
    SELECT * FROM authorisations
)
SELECT person_id, record_id, therapy_date, therapy_source, therapy_type,
    CASE WHEN is_authorisation_only THEN NULL
        -- The annual interval needs a known 5 mg dose; unstrengthened codes get six months.
        WHEN therapy_type = 'ZOLEDRONIC_ACID' AND unit_dose_strength_mg = 5 THEN 12
        ELSE 6 END AS coverage_months,
    mapped_concept_code, mapped_concept_display, date_recorded, evidence_type,
    is_authorisation_only, has_administration_evidence, is_outside_prescription, unit_dose_strength_mg,
    CASE WHEN unit_dose_strength_mg IS NULL THEN NULL
        WHEN therapy_type = 'ZOLEDRONIC_ACID' THEN unit_dose_strength_mg = 4
        -- dm+d rounds Xgeva 120 mg/1.7 ml to 70 mg/ml, giving 119 mg per vial.
        WHEN therapy_type = 'DENOSUMAB' THEN unit_dose_strength_mg IN (119, 120)
    END AS is_oncology_regimen
FROM therapy
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY therapy_source, record_id
    ORDER BY therapy_date DESC, date_recorded DESC NULLS LAST, mapped_concept_code DESC
) = 1
