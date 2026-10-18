-- No caller enables history on either extractor, so exercise both macro branches with synthetic inputs.
{% set orders = get_medication_orders(cluster_id='SYN_CLUSTER', source='SYN_SOURCE', include_history=true) %}
{% set statements = get_medication_statements(cluster_id='SYN_CLUSTER', source='SYN_SOURCE', include_history=true) %}
{% set replacements = {
    'stg_reference_combined_codesets': 'synthetic_codes',
    'stg_nhsd_snomed_sct_history': 'synthetic_history',
    'stg_olids_medication_order': 'synthetic_orders',
    'stg_olids_medication_statement': 'synthetic_statements',
    'int_patient_person_unique': 'synthetic_patients'
} %}
{% set queries = namespace(orders=orders, statements=statements) %}
{% for model, synthetic_relation in replacements.items() %}
    {% set queries.orders = queries.orders | replace(ref(model) | string, synthetic_relation) %}
    {% set queries.statements = queries.statements | replace(ref(model) | string, synthetic_relation) %}
{% endfor %}

WITH synthetic_codes AS (
    SELECT column1::VARCHAR AS code, 'SYN_CLUSTER'::VARCHAR AS cluster_id,
        'SYN_SOURCE'::VARCHAR AS source, 'Synthetic cluster'::VARCHAR AS cluster_description
    FROM VALUES ('SYN_A'), ('SYN_B')
),
synthetic_history AS (
    SELECT column1::VARCHAR AS old_concept_id, column2::VARCHAR AS new_concept_id,
        TRUE AS is_ambiguous
    FROM VALUES ('SYN_OK', 'SYN_A'), ('SYN_OK', 'SYN_B'),
        ('SYN_BAD', 'SYN_A'), ('SYN_BAD', 'SYN_OUTSIDE')
),
synthetic_patients AS (
    SELECT column1::VARCHAR AS patient_id, column2::NUMBER AS person_id,
        column2::NUMBER AS sk_patient_id
    FROM VALUES ('SYN_PAT_OK', -18004), ('SYN_PAT_BAD', -18005)
),
synthetic_events AS (
    SELECT column1::VARCHAR AS id, column2::VARCHAR AS patient_id,
        column3::VARCHAR AS mapped_concept_code,
        '2024-01-01'::TIMESTAMP_NTZ AS clinical_effective_date,
        '2024-01-02'::TIMESTAMP_NTZ AS date_recorded,
        'Synthetic medication'::VARCHAR AS medication_name,
        NULL::VARCHAR AS mapped_concept_display, NULL::VARCHAR AS dose,
        NULL::NUMBER AS quantity_value, NULL::VARCHAR AS quantity_unit,
        NULL::VARCHAR AS bnf_chapter, NULL::VARCHAR AS bnf_section,
        NULL::VARCHAR AS bnf_code, NULL::VARCHAR AS bnf_name
    FROM VALUES ('SYN_OK', 'SYN_PAT_OK', 'SYN_OK'), ('SYN_BAD', 'SYN_PAT_BAD', 'SYN_BAD')
),
synthetic_orders AS (
    SELECT *, NULL::VARCHAR AS medication_statement_id, NULL::NUMBER AS duration_days,
        NULL::NUMBER AS estimated_cost, NULL::VARCHAR AS statement_medication_name
    FROM synthetic_events
),
synthetic_statements AS (
    SELECT *, NULL::VARCHAR AS authorisation_type_code,
        NULL::VARCHAR AS authorisation_type_display, NULL::VARCHAR AS issue_method,
        NULL::BOOLEAN AS is_active, NULL::DATE AS cancellation_date,
        NULL::DATE AS expiry_date, NULL::NUMBER AS age_at_event
    FROM synthetic_events
),
actual_orders AS ({{ queries.orders }}),
actual_statements AS ({{ queries.statements }}),
actual AS (
    SELECT 'ORDER' AS source_kind, medication_order_id AS id, person_id FROM actual_orders
    UNION ALL
    SELECT 'STATEMENT', medication_statement_id, person_id FROM actual_statements
),
actual_counts AS (
    SELECT source_kind, id, person_id, COUNT(*) AS row_count
    FROM actual
    GROUP BY source_kind, id, person_id
),
expected AS (
    SELECT column1::VARCHAR AS source_kind, 'SYN_OK'::VARCHAR AS id, -18004::NUMBER AS person_id
    FROM VALUES ('ORDER'), ('STATEMENT')
)
SELECT
    'medication_history_membership_and_grain' AS check_name,
    COALESCE(actual.source_kind, expected.source_kind) AS source_kind,
    actual.id AS actual_id, expected.id AS expected_id,
    actual.person_id AS actual_person_id, expected.person_id AS expected_person_id,
    actual.row_count AS actual_row_count, 1 AS expected_row_count
FROM actual_counts AS actual
FULL OUTER JOIN expected USING (source_kind, id)
WHERE actual.id IS NULL OR expected.id IS NULL
    OR NOT EQUAL_NULL(actual.person_id, expected.person_id)
    OR actual.row_count <> 1
