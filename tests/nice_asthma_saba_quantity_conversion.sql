{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = calculate_nice_asthma_risk_medications() %}
{% set query = query | replace(ref('int_medication_order_bnf') | string, 'synthetic_orders') %}
{% set query = query | replace(ref('stg_reference_combined_codesets') | string, 'synthetic_codes') %}

WITH synthetic_orders AS (
    SELECT -9650::NUMBER AS person_id, column1::VARCHAR AS medication_order_id,
        '2025-01-01'::DATE AS order_date, column2::VARCHAR AS medication_name,
        column3::NUMBER AS quantity_value, column4::VARCHAR AS mapped_concept_code,
        column5::VARCHAR AS bnf_code
    FROM VALUES
        ('SYN_A', 'Synthetic Accuhaler', 120, 'SYN_SABA', '0301011R0'),
        ('SYN_B', 'Synthetic Turbohaler', 200, 'SYN_SABA', '0301011V0'),
        ('SYN_C', 'Synthetic MDI', 400, 'SYN_SABA', '0301011R0'),
        ('SYN_D', 'Synthetic MDI', NULL, 'SYN_SABA', '0301011R0'),
        ('SYN_E', 'Synthetic prednisolone', 30, 'SYN_OCS', '0603020T0')
), synthetic_codes AS (
    SELECT 'SYN_SABA'::VARCHAR AS code, 'OPENCODELISTS'::VARCHAR AS source,
        'OPENSAFELY/SABA_INHALER_MEDICATIONS'::VARCHAR AS cluster_id
    FROM VALUES (1), (2)
), actual AS (
    {{ query }}
), expected AS (
    SELECT column1::VARCHAR AS medication_order_id, column2::NUMBER AS saba_inhaler_count,
        column3::BOOLEAN AS is_prednisolone
    FROM VALUES ('SYN_A', 2, FALSE), ('SYN_B', 2, FALSE), ('SYN_C', 2, FALSE),
        ('SYN_D', 1, FALSE), ('SYN_E', 0, TRUE)
), actual_occurrences AS (
    SELECT medication_order_id, saba_inhaler_count, is_prednisolone, COUNT(*) AS occurrences
    FROM actual GROUP BY ALL
), expected_occurrences AS (
    SELECT *, COUNT(*) AS occurrences FROM expected GROUP BY ALL
), failures AS (
    (SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
    UNION ALL
    (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences)
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
