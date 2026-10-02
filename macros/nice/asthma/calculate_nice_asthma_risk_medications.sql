{% macro calculate_nice_asthma_risk_medications() %}
WITH saba_codes AS (
    SELECT DISTINCT code
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'OPENCODELISTS'
        AND cluster_id = 'OPENSAFELY/SABA_INHALER_MEDICATIONS'
)
SELECT m.person_id, m.medication_order_id, m.order_date::DATE AS order_date,
    m.bnf_code LIKE '0603020T0%' AS is_prednisolone,
    -- Quantities are doses: Accuhaler has 60, Turbohaler 100, other inhalers 200.
    -- Retain the research convention of at least one device per matched order.
    IFF(s.code IS NOT NULL,
        GREATEST(1, ROUND(COALESCE(m.quantity_value, 0) /
            CASE WHEN m.medication_name ILIKE '%accuhaler%' THEN 60
                 WHEN m.medication_name ILIKE '%turbohaler%' THEN 100
                 ELSE 200 END)), 0)::NUMBER AS saba_inhaler_count
FROM {{ ref('int_medication_order_bnf') }} m
LEFT JOIN saba_codes s ON m.mapped_concept_code = s.code
WHERE s.code IS NOT NULL OR m.bnf_code LIKE '0603020T0%'
{% endmacro %}
